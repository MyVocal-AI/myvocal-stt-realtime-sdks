// Lifecycle: create a session, issue a browser ticket (helper only), stream, pause, resume (a
// new epoch), finish twice (same result) and read the session back over REST.
//
//	MYVOCAL_ACCESS_KEY=... go run ./examples/pause_resume speech.wav
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

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
	enc, err := info.Encoding()
	if err != nil {
		log.Fatal(err)
	}
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
		Title: "pause/resume example", Options: &sttrealtime.SessionOptions{InputEncoding: enc}}})
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// Ticket helper: hand ticket.Value to YOUR browser front end; never log it.
	ticket, err := client.CreateTicket(ctx, session.SessionID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("ticket issued:", ticket.TicketID, "expires in", ticket.ExpiresInSeconds, "s")

	third := len(pcm) / 3 / 2 * 2
	must(session.SendReader(ctx, bytes.NewReader(pcm[:third]), true))
	must(session.Pause(ctx, sttrealtime.ViaWS)) // flushes and confirms first; nothing is sent while paused
	fmt.Println("paused at epoch", session.Cursors().Epoch)
	time.Sleep(time.Second)                      // paused time is never sent as audio
	must(session.Resume(ctx, sttrealtime.ViaWS)) // or sttrealtime.ViaREST
	fmt.Println("resumed at epoch", session.Cursors().Epoch)
	must(session.SendReader(ctx, bytes.NewReader(pcm[third:]), true))
	result, err := session.Finish(ctx)
	must(err)
	again, _ := session.Finish(ctx) // same result, no second settlement
	if again != result {
		log.Fatal("repeated Finish returned a different result")
	}
	view, err := client.GetSession(ctx, result.SessionID)
	must(err)
	billable := int64(-1)
	if result.Usage.BillableCharacters != nil {
		billable = *result.Usage.BillableCharacters
	}
	fmt.Println("status:", result.Status, "/ GET:", view.Status, "| epochs:", view.Epoch, "| billable:", billable,
		"| text:", result.Text())
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
