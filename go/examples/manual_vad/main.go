// Manual segment commits versus server VAD.
//
//	MYVOCAL_ACCESS_KEY=... go run ./examples/manual_vad speech.wav manual|vad
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"

	sttrealtime "github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go"
)

func readWAV(path string) (string, []byte) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	info, err := sttrealtime.ReadWAVHeader(f)
	if err != nil {
		log.Fatal(err)
	}
	enc, err := info.Encoding()
	if err != nil {
		log.Fatal(err)
	}
	pcm, err := io.ReadAll(io.LimitReader(f, info.DataSize))
	if err != nil {
		log.Fatal(err)
	}
	return enc, pcm
}

func main() {
	mode := "manual"
	if len(os.Args) > 2 {
		mode = os.Args[2]
	}
	enc, pcm := readWAV(os.Args[1])
	opts := &sttrealtime.SessionOptions{InputEncoding: enc, SegmentCommitMode: mode}
	if mode == "vad" {
		opts.CommitSilenceMs = sttrealtime.Float(600)
	}
	client, err := sttrealtime.NewClient(sttrealtime.Config{AccessKey: os.Getenv("MYVOCAL_ACCESS_KEY"),
		BaseURL: os.Getenv("MYVOCAL_API_BASE")})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	session, err := client.Connect(ctx, sttrealtime.ConnectOptions{
		Request:  &sttrealtime.SessionRequest{Title: "manual/vad example", Options: opts},
		Handlers: map[string]func(sttrealtime.Event){sttrealtime.EventTranscriptFinal: func(e sttrealtime.Event) { fmt.Println("final:", e.Text()) }}})
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	half := len(pcm) / 2
	if err := session.SendReader(ctx, bytes.NewReader(pcm[:half]), true); err != nil {
		log.Fatal(err)
	}
	if mode == "manual" {
		if err := session.Commit(ctx); err != nil { // closes a line and keeps recording, same epoch
			log.Fatal(err)
		}
	}
	if err := session.SendReader(ctx, bytes.NewReader(pcm[half:]), true); err != nil {
		log.Fatal(err)
	}
	result, err := session.Finish(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(mode, "->", result.Status, len(result.Lines), "lines |", result.Text())
}
