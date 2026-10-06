package sttrealtime

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// Format is a public inputEncoding with its sample rate and sample width.
type Format struct {
	Encoding       string
	SampleRateHz   int
	BytesPerSample int
}

// Encodings lists the seven public input encodings.
var Encodings = map[string]Format{
	"pcm_s16le_8000":  {"pcm_s16le_8000", 8000, 2},
	"pcm_s16le_16000": {"pcm_s16le_16000", 16000, 2},
	"pcm_s16le_22050": {"pcm_s16le_22050", 22050, 2},
	"pcm_s16le_24000": {"pcm_s16le_24000", 24000, 2},
	"pcm_s16le_44100": {"pcm_s16le_44100", 44100, 2},
	"pcm_s16le_48000": {"pcm_s16le_48000", 48000, 2},
	"mulaw_8000":      {"mulaw_8000", 8000, 1},
}

// DefaultEncoding is the server default inputEncoding.
const DefaultEncoding = "pcm_s16le_16000"

// FormatOf resolves an inputEncoding ("" means the default).
func FormatOf(encoding string) (Format, error) {
	key := strings.ToLower(strings.TrimSpace(encoding))
	if key == "" {
		key = DefaultEncoding
	}
	f, ok := Encodings[key]
	if !ok {
		return Format{}, &AudioFormatError{Msg: fmt.Sprintf("unknown inputEncoding %q", encoding)}
	}
	return f, nil
}

// WAVInfo describes a RIFF/WAVE header and where its data chunk is.
type WAVInfo struct {
	AudioFormat   int
	Channels      int
	SampleRateHz  int
	BitsPerSample int
	DataOffset    int64
	DataSize      int64
}

// Encoding returns the matching inputEncoding for PCM16 mono or 8 kHz mu-law mono WAV.
func (w WAVInfo) Encoding() (string, error) {
	if w.Channels != 1 {
		return "", &AudioFormatError{Msg: fmt.Sprintf("WAV must be mono, found %d channels", w.Channels)}
	}
	var name string
	switch {
	case w.AudioFormat == 1 && w.BitsPerSample == 16:
		name = fmt.Sprintf("pcm_s16le_%d", w.SampleRateHz)
	case w.AudioFormat == 7 && w.BitsPerSample == 8 && w.SampleRateHz == 8000:
		name = "mulaw_8000"
	default:
		return "", &AudioFormatError{Msg: fmt.Sprintf("WAV format %d/%d-bit/%d Hz has no MyVocal inputEncoding",
			w.AudioFormat, w.BitsPerSample, w.SampleRateHz)}
	}
	if _, ok := Encodings[name]; !ok {
		return "", &AudioFormatError{Msg: fmt.Sprintf("WAV sample rate %d Hz has no MyVocal inputEncoding", w.SampleRateHz)}
	}
	return name, nil
}

// ReadWAVHeader reads a RIFF/WAVE header and leaves r positioned at the start of the data chunk.
func ReadWAVHeader(r io.Reader) (WAVInfo, error) {
	var riff [12]byte
	if _, err := io.ReadFull(r, riff[:]); err != nil || string(riff[0:4]) != "RIFF" || string(riff[8:12]) != "WAVE" {
		return WAVInfo{}, &AudioFormatError{Msg: "not a RIFF/WAVE file"}
	}
	var info WAVInfo
	haveFmt := false
	pos := int64(12)
	for {
		var head [8]byte
		if _, err := io.ReadFull(r, head[:]); err != nil {
			return WAVInfo{}, &AudioFormatError{Msg: "WAV has no data chunk"}
		}
		id := string(head[0:4])
		size := int64(binary.LittleEndian.Uint32(head[4:8]))
		pos += 8
		switch id {
		case "fmt ":
			body := make([]byte, size)
			if _, err := io.ReadFull(r, body); err != nil || size < 16 {
				return WAVInfo{}, &AudioFormatError{Msg: "WAV fmt chunk is truncated"}
			}
			info.AudioFormat = int(binary.LittleEndian.Uint16(body[0:2]))
			info.Channels = int(binary.LittleEndian.Uint16(body[2:4]))
			info.SampleRateHz = int(binary.LittleEndian.Uint32(body[4:8]))
			info.BitsPerSample = int(binary.LittleEndian.Uint16(body[14:16]))
			haveFmt = true
			pos += size
		case "data":
			if !haveFmt {
				return WAVInfo{}, &AudioFormatError{Msg: "WAV data chunk before fmt chunk"}
			}
			info.DataOffset, info.DataSize = pos, size
			return info, nil
		default:
			if _, err := io.CopyN(io.Discard, r, size); err != nil {
				return WAVInfo{}, &AudioFormatError{Msg: "WAV is truncated"}
			}
			pos += size
		}
		if size%2 == 1 {
			if _, err := io.CopyN(io.Discard, r, 1); err != nil {
				return WAVInfo{}, &AudioFormatError{Msg: "WAV is truncated"}
			}
			pos++
		}
	}
}
