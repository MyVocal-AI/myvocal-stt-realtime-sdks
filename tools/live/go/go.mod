module example.com/myvocal-stt-live

go 1.22

require github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go v1.0.0

require github.com/gorilla/websocket v1.5.3 // indirect

// The matrix/live runner points this at the clean candidate export; the default is the repo copy.
replace github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go => ../../../go
