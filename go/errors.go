package sttrealtime

import (
	"errors"
	"fmt"
	"regexp"
)

// Sentinel kinds usable with errors.Is on an *APIError.
var (
	ErrAuthentication         = errors.New("myvocal: authentication failed")
	ErrInputInvalid           = errors.New("myvocal: INPUT_INVALID")
	ErrIdempotencyConflict    = errors.New("myvocal: IDEMPOTENCY_CONFLICT")
	ErrInsufficientCharacters = errors.New("myvocal: INSUFFICIENT_CHARACTERS")
	ErrNotFound               = errors.New("myvocal: PROJECT_NOT_FOUND")
)

// APIError is a MyVocal envelope with code != 1 (HTTP 200 included). It never carries the
// access key, a ticket, an auth header or a signed URL.
type APIError struct {
	Code      int64
	ErrorCode string
	Message   string
	Retryable bool
	Action    string
	RequestID string
	Details   map[string]any
}

func (e *APIError) Error() string {
	return fmt.Sprintf("myvocal: API error code=%d errorCode=%s retryable=%t requestId=%s",
		e.Code, e.ErrorCode, e.Retryable, e.RequestID)
}

// Is maps the error onto the sentinel kinds.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrAuthentication:
		return e.Code == 401 || e.ErrorCode == "UNAUTHORIZED"
	case ErrInputInvalid:
		return e.ErrorCode == "INPUT_INVALID"
	case ErrIdempotencyConflict:
		return e.ErrorCode == "IDEMPOTENCY_CONFLICT"
	case ErrInsufficientCharacters:
		return e.ErrorCode == "INSUFFICIENT_CHARACTERS"
	case ErrNotFound:
		return e.ErrorCode == "PROJECT_NOT_FOUND"
	}
	return false
}

// TransportError is a failure below the MyVocal envelope (network, TLS, proxy, HTTP status).
type TransportError struct {
	Op        string
	Status    int // HTTP status when known
	SessionID string
	Err       error
}

func (e *TransportError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("myvocal: %s: HTTP %d", e.Op, e.Status)
	}
	if e.Err != nil {
		return fmt.Sprintf("myvocal: %s: %s", e.Op, safeErr(e.Err))
	}
	return "myvocal: " + e.Op
}
func (e *TransportError) Unwrap() error { return e.Err }

// AudioFormatError: audio does not fit the session encoding.
type AudioFormatError struct{ Msg string }

func (e *AudioFormatError) Error() string { return "myvocal: audio: " + e.Msg }

// SessionStateError: the operation is not allowed in the local session state.
type SessionStateError struct{ Msg string }

func (e *SessionStateError) Error() string { return "myvocal: session state: " + e.Msg }

// ConnectionLostError: the socket ended before the session finished; no audio is accepted.
type ConnectionLostError struct {
	SessionID string
	Msg       string
}

func (e *ConnectionLostError) Error() string {
	return fmt.Sprintf("myvocal: %s (sessionId=%s)", e.Msg, e.SessionID)
}

// BackpressureTimeoutError: the bounded buffer stayed full without acknowledgement progress.
type BackpressureTimeoutError struct{ SessionID string }

func (e *BackpressureTimeoutError) Error() string {
	return "myvocal: send buffer full without acknowledgement progress (sessionId=" + e.SessionID + ")"
}

// AckTimeoutError: unacknowledged audio made no progress within Timeouts.Ack.
type AckTimeoutError struct {
	SessionID string
	What      string
}

func (e *AckTimeoutError) Error() string {
	return "myvocal: no acknowledgement progress for " + e.What + " (sessionId=" + e.SessionID + ")"
}

// ServerEventError: session.error that stopped the stream.
type ServerEventError struct {
	ErrorCode string
	SessionID string
}

func (e *ServerEventError) Error() string {
	return "myvocal: session.error " + e.ErrorCode + " (sessionId=" + e.SessionID + ")"
}

// Is lets errors.Is(err, ErrInsufficientCharacters) match a stream-level balance stop.
func (e *ServerEventError) Is(target error) bool {
	return (target == ErrInsufficientCharacters && e.ErrorCode == "INSUFFICIENT_CHARACTERS") ||
		(target == ErrInputInvalid && e.ErrorCode == "INPUT_INVALID") ||
		(target == ErrNotFound && e.ErrorCode == "PROJECT_NOT_FOUND")
}

// FinishUnconfirmedError: the end of the session could not be confirmed (REST unreachable or
// refused during close-out). Keep SessionID and call GetSession/FinishSession later.
type FinishUnconfirmedError struct {
	SessionID string
	Msg       string
	Cause     error
}

func (e *FinishUnconfirmedError) Error() string {
	return fmt.Sprintf("myvocal: %s (sessionId=%s)", e.Msg, e.SessionID)
}
func (e *FinishUnconfirmedError) Unwrap() error { return e.Cause }

func apiErrorFromEnvelope(env map[string]any) *APIError {
	e := &APIError{Message: str(env["message"])}
	if n, ok := asInt64(env["code"]); ok {
		e.Code = n
	}
	if d, ok := env["data"].(map[string]any); ok {
		e.ErrorCode = str(d["errorCode"])
		e.Retryable, _ = d["retryable"].(bool)
		e.Action = str(d["action"])
		e.RequestID = str(d["requestId"])
		e.Details, _ = d["details"].(map[string]any)
	}
	if e.ErrorCode == "" && e.Code == 401 {
		e.ErrorCode = "UNAUTHORIZED"
	}
	return e
}

// safeErr strips URLs from wrapped network errors (a URL may carry a query string).
func safeErr(err error) string {
	return urlPattern.ReplaceAllString(err.Error(), "<url>")
}

var urlPattern = regexp.MustCompile(`(?i)\b(?:https?|wss?)://[^\s"']+`)
