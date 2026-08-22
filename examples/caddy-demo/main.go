package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var reqCounter atomic.Int64

func main() {
	mux := http.NewServeMux()

	// 1. Time Endpoint
	mux.HandleFunc("GET /api/time", func(w http.ResponseWriter, r *http.Request) {
		count := reqCounter.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=15")
		w.Header().Set("Cache-Tag", "time-tag")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"timestamp":       time.Now().Format(time.RFC3339Nano),
			"origin_exec_seq": count,
			"message":         "Response generated at mock upstream origin",
		})
	})

	// 2. Products Endpoint
	mux.HandleFunc("GET /api/products", func(w http.ResponseWriter, r *http.Request) {
		reqCounter.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=30")
		w.Header().Set("Cache-Tag", "products,catalog")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 1, "name": "Cloud Edge CDN", "price": 49.99},
			{"id": 2, "name": "High-Speed Cache Accelerator", "price": 99.00},
		})
	})

	// 3. Multi-Variant Vary Endpoint
	mux.HandleFunc("GET /api/vary", func(w http.ResponseWriter, r *http.Request) {
		lang := r.Header.Get("Accept-Language")
		if lang == "" {
			lang = "en-US"
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Vary", "Accept-Language, Accept-Encoding")
		w.Header().Set("Cache-Control", "public, max-age=30")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"language": lang,
			"greeting": fmt.Sprintf("Hello! Resolved language variant: %s", lang),
		})
	})

	// 4. Cached User Endpoint
	mux.HandleFunc("GET /api/user/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Cache-Tag", fmt.Sprintf("users,user-%s", id))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"user_id":   id,
			"username":  fmt.Sprintf("user_%s", id),
			"timestamp": time.Now().Format(time.RFC3339),
		})
	})

	// 5. Mutating User Endpoint
	mux.HandleFunc("POST /api/user/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  "updated",
			"user_id": id,
			"message": "User record updated at origin",
		})
	})

	// Root overview
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprintln(w, "Mock Upstream Origin Server running on :9000 (proxied through Caddy on :8080)")
	})

	log.Println("Mock upstream origin server listening on :9000...")
	if err := http.ListenAndServe(":9000", mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
