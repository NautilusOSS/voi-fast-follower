package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
	"github.com/NautilusOSS/voi-fast-follower/internal/voi"
)

// Server exposes a tiny read-only explorer API over the follower DB.
type Server struct {
	DB  *storage.PostgresBlockSink
	Voi *voi.Client
}

// SenderView is a sender row enriched with a live algod balance.
type SenderView struct {
	Sender         string  `json:"sender"`
	TxCount        int64   `json:"tx_count"`
	RoundsActive   int64   `json:"rounds_active"`
	BalanceMicro   uint64  `json:"balance_micro"`
	BalanceVOI     float64 `json:"balance_voi"`
	BalanceKnown   bool    `json:"balance_known"`
}

// Register mounts API routes on mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/blocks", s.handleBlocks)
	mux.HandleFunc("/api/blocks/", s.handleBlock)
	mux.HandleFunc("/api/transactions", s.handleTransactions)
	mux.HandleFunc("/api/senders", s.handleSenders)
	mux.HandleFunc("/api/apps", s.handleApps)
	mux.HandleFunc("/api/busy-blocks", s.handleBusyBlocks)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.DB.Status(r.Context())
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	writeJSON(w, st)
}

func (s *Server) handleBlocks(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	var before uint64
	if v := r.URL.Query().Get("before"); v != "" {
		before, _ = strconv.ParseUint(v, 10, 64)
	}
	blocks, err := s.DB.ListBlocks(r.Context(), limit, before)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"blocks": blocks})
}

func (s *Server) handleBlock(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/blocks/")
	path = strings.Trim(path, "/")
	if path == "" {
		http.NotFound(w, r)
		return
	}
	round, err := strconv.ParseUint(path, 10, 64)
	if err != nil {
		writeErr(w, err, http.StatusBadRequest)
		return
	}
	blk, ok, err := s.DB.GetBlock(r.Context(), round)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	txs, err := s.DB.ListTransactions(r.Context(), 200, round, "", 0)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"block": blk, "transactions": txs})
}

func (s *Server) handleTransactions(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	var round uint64
	if v := r.URL.Query().Get("round"); v != "" {
		round, _ = strconv.ParseUint(v, 10, 64)
	}
	var appID uint64
	if v := r.URL.Query().Get("app_id"); v != "" {
		appID, _ = strconv.ParseUint(v, 10, 64)
	}
	sender := r.URL.Query().Get("sender")
	txs, err := s.DB.ListTransactions(r.Context(), limit, round, sender, appID)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"transactions": txs})
}

func (s *Server) handleSenders(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	senders, err := s.DB.TopSenders(r.Context(), limit)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	views := enrichSenders(r.Context(), s.Voi, senders)
	writeJSON(w, map[string]any{"senders": views})
}

func enrichSenders(ctx context.Context, client *voi.Client, senders []storage.SenderStat) []SenderView {
	out := make([]SenderView, len(senders))
	if len(senders) == 0 {
		return out
	}
	var wg sync.WaitGroup
	for i, sstat := range senders {
		out[i] = SenderView{
			Sender:       sstat.Sender,
			TxCount:      sstat.TxCount,
			RoundsActive: sstat.RoundsActive,
		}
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			bal, err := client.AccountBalance(ctx, addr)
			if err != nil {
				return
			}
			out[i].BalanceMicro = bal
			out[i].BalanceVOI = float64(bal) / 1_000_000
			out[i].BalanceKnown = true
		}(i, sstat.Sender)
	}
	wg.Wait()
	return out
}

func (s *Server) handleApps(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	apps, err := s.DB.TopApps(r.Context(), limit)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"apps": apps})
}

func (s *Server) handleBusyBlocks(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	blocks, err := s.DB.BusyBlocks(r.Context(), limit)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"blocks": blocks})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, err error, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
