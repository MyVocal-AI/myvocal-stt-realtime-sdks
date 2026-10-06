package sttrealtime

import "time"

// Timeouts are the configurable deadlines. Zero values take the defaults below.
type Timeouts struct {
	Request      time.Duration // one REST request (default 30s)
	Create       time.Duration // the whole create call including idempotent retries (30s)
	Connect      time.Duration // socket open + session.ready (15s)
	Ack          time.Duration // unacknowledged audio without progress (10s); idle time never counts
	Finish       time.Duration // after session.finish, waiting for session.completed (60s)
	Closeout     time.Duration // REST close-out after a disconnect/timeout (70s)
	Close        time.Duration // local release in Close (5s)
	Backpressure time.Duration // SendAudio waiting for buffer room (defaults to Ack)
}

func (t Timeouts) withDefaults() Timeouts {
	def := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&t.Request, 30*time.Second)
	def(&t.Create, 30*time.Second)
	def(&t.Connect, 15*time.Second)
	def(&t.Ack, 10*time.Second)
	def(&t.Finish, 60*time.Second)
	def(&t.Closeout, 70*time.Second)
	def(&t.Close, 5*time.Second)
	def(&t.Backpressure, t.Ack)
	return t
}
