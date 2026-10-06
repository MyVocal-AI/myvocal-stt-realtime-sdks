// Abnormal finish: the socket drops mid-stream. The SDK stops taking audio, queries the
// ORIGINAL session and finishes it over REST (no reconnect, no resend, no new session). If the
// end cannot be confirmed, keep the session id and finish it later explicitly.
//
//	MYVOCAL_ACCESS_KEY=... go run ./examples/abnormal_finish speech.wav
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	sttrealtime "github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go"
)

func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	info, err := sttrealtime.ReadWAVHeader(f)
	if err != nil {
		log.Fatal(err)
	}
	enc, _ := info.Encoding()
	pcm, _ := io.ReadAll(io.LimitReader(f, info.DataSize))
	f.Close()

	client, err := sttrealtime.NewClient(sttrealtime.Config{AccessKey: os.Getenv("MYVOCAL_ACCESS_KEY"),
		BaseURL: os.Getenv("MYVOCAL_API_BASE")})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	session, err := client.Connect(ctx, sttrealtime.ConnectOptions{Request: &sttrealtime.SessionRequest{
		Title: "abnormal-finish example", Options: &sttrealtime.SessionOptions{InputEncoding: enc}}})
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	sessionID := session.SessionID

	half := len(pcm) / 2 / 2 * 2
	if err := session.SendReader(ctx, bytes.NewReader(pcm[:half]), true); err != nil {
		log.Fatal(err)
	}
	if err := session.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	session.AbortConnection() // simulates a network failure on this client only
	var lost *sttrealtime.ConnectionLostError
	if err := session.SendAudio(ctx, pcm[half:]); errors.As(err, &lost) {
		fmt.Println("socket lost; audio is no longer accepted")
	}
	result, err := session.Finish(ctx) // REST close-out of the same session
	var unconfirmed *sttrealtime.FinishUnconfirmedError
	switch {
	case err == nil:
		fmt.Println("closed out:", result.Status, "via REST:", result.ViaREST, "reason:", result.CloseoutReason)
	case errors.As(err, &unconfirmed):
		fmt.Println("end not confirmed yet for", unconfirmed.SessionID, "- finish it later:")
		view, gerr := client.GetSession(ctx, sessionID)
		if gerr == nil && !view.IsTerminal() {
			view, gerr = client.FinishSession(ctx, sessionID, nil)
		}
		if gerr != nil {
			log.Fatal(gerr)
		}
		fmt.Println("explicit finish:", view.Status)
	default:
		log.Fatal(err)
	}
}
