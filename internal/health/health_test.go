package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestModesAndHandlers(t *testing.T) {
	tr := New()
	if tr.Snapshot().Mode != ModeStartup {
		t.Fatal(tr.Snapshot().Mode)
	}
	tr.UpdateProgress(100, 200, 99, true)
	if tr.Snapshot().Mode != ModeCatchingUp {
		t.Fatalf("want catching_up got %s", tr.Snapshot().Mode)
	}
	tr.UpdateProgress(200, 200, 199, true)
	if tr.Snapshot().Mode != ModeLive {
		t.Fatalf("want live got %s", tr.Snapshot().Mode)
	}
	tr.SetSinkError("boom")
	if tr.Snapshot().Mode != ModeDegraded {
		t.Fatal(tr.Snapshot().Mode)
	}
	tr.ClearSinkError()
	tr.UpdateProgress(200, 200, 199, true)
	if tr.Snapshot().Mode != ModeLive {
		t.Fatal(tr.Snapshot().Mode)
	}

	mux := http.NewServeMux()
	tr.RegisterHandlers(mux)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("readyz=%d", rr.Code)
	}
	var snap Snapshot
	if err := json.Unmarshal(rr.Body.Bytes(), &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Mode != ModeLive {
		t.Fatal(snap.Mode)
	}

	tr.SetFailed("dead")
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("healthz=%d", rr.Code)
	}
}
