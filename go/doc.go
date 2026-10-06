// Package sttrealtime is the MyVocal Speech to Text Realtime SDK for Go (server side).
//
// Import it as
//
//	import sttrealtime "github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go"
//
// A Client creates sessions over REST and opens each session's WebSocket with the accessKey
// header. A Session frames audio (about 100 ms), keeps the sample cursors, follows server
// rotation by resending only unconfirmed bytes, serializes lifecycle operations and, on an
// unexpected disconnect, closes out the original session over REST. It never reconnects,
// resends audio into a new task or creates a second session.
//
// 64-bit sample, usage and millisecond fields are decimal strings on the wire and int64 here.
// nil pointers mean "absent" (never 0). Event sequence numbers belong to one connection.
package sttrealtime

// Version of this SDK.
const Version = "1.0.0"

const userAgent = "myvocal-stt-realtime-go/" + Version

const realtimePath = "/sound_clone/api/v1/stt/realtime"
