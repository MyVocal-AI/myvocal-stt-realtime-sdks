// Streams a WAV (16-bit PCM mono, or 8 kHz mu-law) file at real-time pace and prints the transcript.
//
//	MYVOCAL_ACCESS_KEY=... go run ./examples/file_stream speech.wav [languageHint]
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	sttrealtime "github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go"
)

func main() {
	path := os.Args[1]
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	info, err := sttrealtime.ReadWAVHeader(f)
	f.Close()
	if err != nil {
		log.Fatal(err)
	}
	encoding, err := info.Encoding() // e.g. pcm_s16le_16000
	if err != nil {
		log.Fatal(err)
	}
	client, err := sttrealtime.NewClient(sttrealtime.Config{AccessKey: os.Getenv("MYVOCAL_ACCESS_KEY"),
		BaseURL: os.Getenv("MYVOCAL_API_BASE")})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	req := &sttrealtime.SessionRequest{Title: "file-stream example",
		Options: &sttrealtime.SessionOptions{InputEncoding: encoding, SegmentCommitMode: "vad"}}
	if len(os.Args) > 2 {
		req.LanguageHint = os.Args[2]
	}
	ctx := context.Background()
	session, err := client.Connect(ctx, sttrealtime.ConnectOptions{Request: req, Handlers: map[string]func(sttrealtime.Event){
		sttrealtime.EventTranscriptDelta: func(e sttrealtime.Event) { fmt.Println("  ...", e.Text()) },
		sttrealtime.EventTranscriptFinal: func(e sttrealtime.Event) { fmt.Println("final:", e.Text()) },
	}})
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()                                     // local resources only
	if err := session.SendFile(ctx, path, true); err != nil { // paced by sample time; ~100 ms frames
		log.Fatal(err)
	}
	result, err := session.Finish(ctx) // flush, wait for acknowledgements, terminal result
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("status:", result.Status, "| transcriptionId:", result.TranscriptionID)
	if result.Usage.BillableCharacters != nil {
		fmt.Println("billableCharacters:", *result.Usage.BillableCharacters)
	}
	fmt.Println("text:", result.Text())
}
