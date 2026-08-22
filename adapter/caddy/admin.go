package caddy

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/caddyserver/caddy/v2"

	"github.com/indragunawan/titip"
)

func init() {
	caddy.RegisterModule(AdminAPI{})
}

// AdminAPI implements the Caddy AdminRouter interface for Titip purge operations.
type AdminAPI struct{}

// CaddyModule returns the Caddy module information.
func (AdminAPI) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "admin.api.titip",
		New: func() caddy.Module { return new(AdminAPI) },
	}
}

// Routes returns the admin routes exposed by Titip.
func (a *AdminAPI) Routes() []caddy.AdminRoute {
	return []caddy.AdminRoute{
		{
			Pattern: "/titip/purge",
			Handler: caddy.AdminHandlerFunc(handleAdminPurge),
		},
	}
}

type purgeAdminRequest struct {
	URLs            []string `json:"urls,omitempty"`
	Tags            []string `json:"tags,omitempty"`
	PurgeEverything bool     `json:"purge_everything,omitempty"`
	Soft            *bool    `json:"soft,omitempty"`
}

type purgeAdminResponse struct {
	Success bool            `json:"success"`
	Purged  adminPurgedInfo `json:"purged"`
}

type adminPurgedInfo struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
	Soft  bool   `json:"soft"`
}

func handleAdminPurge(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed; use POST"}`, http.StatusMethodNotAllowed)
		return nil
	}

	ct := r.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") && ct != "" {
		http.Error(w, `{"error":"unsupported media type; use Content-Type: application/json"}`, http.StatusUnsupportedMediaType)
		return nil
	}

	var req purgeAdminRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON request body"}`, http.StatusBadRequest)
		return nil
	}

	// Count active targets for single-target mutual exclusivity enforcement
	targets := 0
	targetType := ""
	if len(req.URLs) > 0 {
		targets++
		targetType = "urls"
	}
	if len(req.Tags) > 0 {
		targets++
		targetType = "tags"
	}
	if req.PurgeEverything {
		targets++
		targetType = "purge_everything"
	}

	if targets > 1 {
		http.Error(w, `{"error":"specify exactly one of: urls, tags, or purge_everything"}`, http.StatusBadRequest)
		return nil
	}
	if targets == 0 {
		http.Error(w, `{"error":"missing purge target: specify urls, tags, or purge_everything"}`, http.StatusBadRequest)
		return nil
	}

	soft := true
	if req.Soft != nil {
		soft = *req.Soft
	}

	var purgeOpts []titip.PurgeOption
	if soft {
		purgeOpts = append(purgeOpts, titip.WithSoftPurge())
	}

	activeEngines := getEngines()
	count := 0

	switch targetType {
	case "urls":
		for _, u := range req.URLs {
			for _, engine := range activeEngines {
				_ = engine.PurgeURL(r.Context(), u, purgeOpts...)
			}
			count++
		}
	case "tags":
		for _, tag := range req.Tags {
			for _, engine := range activeEngines {
				_ = engine.PurgeTag(r.Context(), tag, purgeOpts...)
			}
			count++
		}
	case "purge_everything":
		// Mark everything purged across all engines
		for _, engine := range activeEngines {
			_ = engine.PurgeTag(r.Context(), "*", purgeOpts...)
		}
		count = 1
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(purgeAdminResponse{
		Success: true,
		Purged: adminPurgedInfo{
			Type:  targetType,
			Count: count,
			Soft:  soft,
		},
	})
	return nil
}

// Interface guards
var (
	_ caddy.Module      = (*AdminAPI)(nil)
	_ caddy.AdminRouter = (*AdminAPI)(nil)
)
