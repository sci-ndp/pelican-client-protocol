package eventsource

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// cephDocsSample is the example notification from the Ceph RGW docs.
const cephDocsSample = `{"Records":[{
  "eventVersion":"2.1","eventSource":"ceph:s3","awsRegion":"zonegroup1",
  "eventTime":"2019-11-22T13:47:35.124724Z","eventName":"ObjectCreated:Put",
  "userIdentity":{"principalId":"tester"},
  "requestParameters":{"sourceIPAddress":""},
  "responseElements":{"x-amz-request-id":"503a4c37.5330.903595","x-amz-id-2":"14d2-zone1-zonegroup1"},
  "s3":{"s3SchemaVersion":"1.0","configurationId":"mynotif1",
    "bucket":{"name":"mybucket1","ownerIdentity":{"principalId":"tester"},"arn":"arn:aws:s3:zonegroup1::mybucket1","id":"503a4c37.5332.38"},
    "object":{"key":"myimage1.jpg","size":"1024","eTag":"37b51d194a7513e45b56f6524f2d51f2","versionId":"","sequencer":"F7E6D75DC742D108","metadata":[],"tags":[]}},
  "eventId":"","opaqueData":"me@example.com"}]}`

func post(h http.Handler, method, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/s3-events", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestS3NotifySource_SingleRecord(t *testing.T) {
	s := NewS3NotifySource(discardLogger())
	if rec := post(s, http.MethodPost, cephDocsSample); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var ev s3Event
	if err := json.Unmarshal([]byte(<-s.Events()), &ev); err != nil {
		t.Fatal(err)
	}
	want := s3Event{
		Event: "ObjectCreated:Put", Bucket: "mybucket1", Key: "myimage1.jpg", Size: 1024,
		ETag: "37b51d194a7513e45b56f6524f2d51f2", Time: "2019-11-22T13:47:35.124724Z",
		Sequencer: "F7E6D75DC742D108",
	}
	if ev != want {
		t.Errorf("event = %+v, want %+v", ev, want)
	}
}

func TestS3NotifySource_MultiRecordInOrder(t *testing.T) {
	s := NewS3NotifySource(discardLogger())
	body := `{"Records":[
	  {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b"},"object":{"key":"one","size":"1"}}},
	  {"eventName":"ObjectRemoved:Delete","s3":{"bucket":{"name":"b"},"object":{"key":"two","size":"2"}}}]}`
	if rec := post(s, http.MethodPost, body); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	for _, key := range []string{"one", "two"} {
		var ev s3Event
		if err := json.Unmarshal([]byte(<-s.Events()), &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Key != key {
			t.Errorf("key = %q, want %q", ev.Key, key)
		}
	}
}

func TestS3NotifySource_Rejections(t *testing.T) {
	s := NewS3NotifySource(discardLogger())
	if rec := post(s, http.MethodGet, ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", rec.Code)
	}
	if rec := post(s, http.MethodPost, "{not json"); rec.Code != http.StatusBadRequest {
		t.Errorf("bad JSON status = %d, want 400", rec.Code)
	}
	huge := `{"Records":[],"pad":"` + strings.Repeat("x", s3NotifyMaxBody) + `"}`
	if rec := post(s, http.MethodPost, huge); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized status = %d, want 413", rec.Code)
	}
	if len(s.Events()) != 0 {
		t.Error("rejected requests must not emit events")
	}
}

func TestS3NotifySource_BackedUpReturns503(t *testing.T) {
	s := NewS3NotifySource(discardLogger())
	for len(s.ch) < cap(s.ch) {
		s.ch <- "filler"
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/s3-events", strings.NewReader(cephDocsSample)).WithContext(ctx)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}
