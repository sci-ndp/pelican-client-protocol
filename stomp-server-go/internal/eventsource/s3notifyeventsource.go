package eventsource

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
)

// s3NotifyMaxBody caps a single notification POST. RGW batches a handful of
// records per request, so this is generous.
const s3NotifyMaxBody = 1 << 20

// S3NotifySource is an EventSource fed by Ceph RadosGW bucket notifications.
// RGW is configured with a topic whose push-endpoint points at this source's
// http.Handler and POSTs JSON "Records" batches to it; each record becomes
// one event, a normalized JSON string (see s3Event).
//
// The handler replies 200 only once every record in the request has been
// handed to Events(), so a persistent RGW topic keeps retrying while this
// process is down or backed up. Delivery is at-least-once: RGW retries can
// redeliver a record, and no deduplication is done here. Pushes are not
// authenticated.
type S3NotifySource struct {
	defaultNotifier
	log *slog.Logger
	ch  chan string
}

func NewS3NotifySource(log *slog.Logger) *S3NotifySource {
	return &S3NotifySource{log: log, ch: make(chan string, 64)}
}

func (s *S3NotifySource) Events() <-chan string { return s.ch }

// s3NotifyPayload is the subset of RGW's notification JSON this source reads.
// Size is a json.Number because RGW emits it as a quoted string.
type s3NotifyPayload struct {
	Records []struct {
		EventName string `json:"eventName"`
		EventTime string `json:"eventTime"`
		EventID   string `json:"eventId"`
		S3        struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key       string      `json:"key"`
				Size      json.Number `json:"size"`
				ETag      string      `json:"eTag"`
				VersionID string      `json:"versionId"`
				Sequencer string      `json:"sequencer"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

// s3Event is the JSON body emitted for each notification record.
type s3Event struct {
	Event     string `json:"event"`
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	Size      int64  `json:"size"`
	ETag      string `json:"etag"`
	VersionID string `json:"version_id"`
	Time      string `json:"time"`
	Sequencer string `json:"sequencer"`
	EventID   string `json:"event_id"`
}

func (s *S3NotifySource) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s3NotifyMaxBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}
	var payload s3NotifyPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		// 4xx so RGW doesn't retry a payload that can never parse.
		s.log.Warn("rejecting malformed S3 notification", "error", err)
		http.Error(w, "malformed notification", http.StatusBadRequest)
		return
	}

	for _, rec := range payload.Records {
		size, _ := rec.S3.Object.Size.Int64()
		out, err := json.Marshal(s3Event{
			Event:     rec.EventName,
			Bucket:    rec.S3.Bucket.Name,
			Key:       rec.S3.Object.Key,
			Size:      size,
			ETag:      rec.S3.Object.ETag,
			VersionID: rec.S3.Object.VersionID,
			Time:      rec.EventTime,
			Sequencer: rec.S3.Object.Sequencer,
			EventID:   rec.EventID,
		})
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		select {
		case s.ch <- string(out):
		case <-r.Context().Done():
			// Consumer is backed up or the peer gave up; a non-2xx makes
			// RGW redeliver the batch rather than silently losing events.
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}
