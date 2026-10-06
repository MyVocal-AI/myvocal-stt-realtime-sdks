// Byte stream: audio arrives in arbitrary-size chunks at capture speed (simulated from a raw
// file); events are consumed from a channel.
//
//	MYVOCAL_ACCESS_KEY=... go run ./examples/byte_stream audio.raw [inputEncoding]
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	sttrealtime "github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go"
)

func main() {
	encoding := "pcm_s16le_16000"
	if len(os.Args) > 2 {
		encoding = os.Args[2]
	}
	client, err := sttrealtime.NewClient(sttrealtime.Config{AccessKey: os.Getenv("MYVOCAL_ACCESS_KEY"),
		BaseURL: os.Getenv("MYVOCAL_API_BASE")})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	session, err := client.Connect(ctx, sttrealtime.ConnectOptions{EventChannel: true,
		Request: &sttrealtime.SessionRequest{Title: "byte-stream example",
			Options: &sttrealtime.SessionOptions{InputEncoding: encoding, SegmentCommitMode: "vad"}}})
	if err != nil {
		log.Fatal(err)
	}
	printed := make(chan struct{})
	go func() {
		defer close(printed)
		for e := range session.Events() {
			switch e.Type {
			case sttrealtime.EventTranscriptFinal:
				fmt.Println("final:", e.Text())
			case sttrealtime.EventSessionNotice:
				n, _ := e.Notice()
				fmt.Println("notice:", n.Code)
			}
		}
	}()
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	format := session.Format
	chunk := make([]byte, format.SampleRateHz*37/1000*format.BytesPerSample+1) // odd size: half samples are fine
	for {
		n, err := f.Read(chunk)
		if n > 0 {
			// a live source is already at capture speed, so the SDK does not sleep again
			if err := session.SendAudio(ctx, chunk[:n]); err != nil {
				log.Fatal(err)
			}
			time.Sleep(time.Duration(float64(n/format.BytesPerSample) / float64(format.SampleRateHz) * float64(time.Second)))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	result, err := session.Finish(ctx)
	if err != nil {
		log.Fatal(err)
	}
	session.Close()
	<-printed
	fmt.Println("status:", result.Status, "| transcriptionId:", result.TranscriptionID, "| text:", result.Text())
}
