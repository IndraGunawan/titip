package caddy

import (
	"encoding/json"
	"slices"
	"testing"

	caddymain "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/indragunawan/titip"
)

func TestCaddyHandler_UnmarshalCaddyfile_Directives(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   string
		validate func(t *testing.T, h *Handler)
	}{
		{
			name: "basic directives",
			config: `titip {
				cache_status RFC9211
				background_fetch_timeout 20s
				storage_timeout 5s
				storage test
				tag_header X-Cache-Tag
				respect_client_cache_control true
				auto_invalidate_mutating_methods true
				use_rewritten_url true
			}`,
			validate: func(t *testing.T, h *Handler) {
				if h.CacheStatus != "RFC9211" {
					t.Errorf("expected cache_status RFC9211, got %s", h.CacheStatus)
				}
				if h.BackgroundFetchTimeout != "20s" {
					t.Errorf("expected background_fetch_timeout 20s, got %s", h.BackgroundFetchTimeout)
				}
				if h.StorageTimeout != "5s" {
					t.Errorf("expected storage_timeout 5s, got %s", h.StorageTimeout)
				}
				if h.TagHeader != "X-Cache-Tag" {
					t.Errorf("expected tag_header X-Cache-Tag, got %s", h.TagHeader)
				}
				if h.RespectClientCacheControl == nil || !*h.RespectClientCacheControl {
					t.Errorf("expected respect_client_cache_control true, got %v", h.RespectClientCacheControl)
				}
				if h.AutoInvalidateMutatingMethods == nil || !*h.AutoInvalidateMutatingMethods {
					t.Errorf("expected auto_invalidate_mutating_methods true, got %v", h.AutoInvalidateMutatingMethods)
				}
				if h.UseRewrittenURL == nil || !*h.UseRewrittenURL {
					t.Errorf("expected use_rewritten_url true, got %v", h.UseRewrittenURL)
				}
			},
		},
		{
			name: "convert_head_to_get false",
			config: `titip {
				convert_head_to_get false
				storage test
			}`,
			validate: func(t *testing.T, h *Handler) {
				if h.ConvertHeadToGet == nil || *h.ConvertHeadToGet != false {
					t.Fatalf("expected ConvertHeadToGet false, got %v", h.ConvertHeadToGet)
				}
			},
		},
		{
			name: "convert_head_to_get true",
			config: `titip {
				convert_head_to_get true
				storage test
			}`,
			validate: func(t *testing.T, h *Handler) {
				if h.ConvertHeadToGet == nil || *h.ConvertHeadToGet != true {
					t.Fatalf("expected ConvertHeadToGet true, got %v", h.ConvertHeadToGet)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := caddyfile.NewTestDispenser(tc.config)
			var h Handler
			if err := h.UnmarshalCaddyfile(d); err != nil {
				t.Fatalf("unmarshal error: %v", err)
			}
			tc.validate(t, &h)
		})
	}
}

func TestCaddyHandler_CacheKey_UnmarshalCaddyfile(t *testing.T) {
	t.Parallel()
	config := `titip {
		storage test
		use_rewritten_url true
		cache_key {
			include_protocol false
			exclude_host true
			exclude_query_string false
			disable_query_string_sort false
			included_query_params id category page
			excluded_query_params tracking
			exclude_marketing_params true
			included_header_names X-App-Version Accept-Language
			included_cookie_names session_currency
			case_insensitive_path true
			included_query_param_values format json xml
		}
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if h.UseRewrittenURL == nil || *h.UseRewrittenURL != true {
		t.Errorf("expected UseRewrittenURL true, got %v", h.UseRewrittenURL)
	}
	if h.CacheKey == nil {
		t.Fatalf("expected CacheKey config to be populated")
	}
	if h.CacheKey.IncludeProtocol == nil || *h.CacheKey.IncludeProtocol != false {
		t.Errorf("expected IncludeProtocol false, got %v", h.CacheKey.IncludeProtocol)
	}
	if h.CacheKey.ExcludeHost == nil || *h.CacheKey.ExcludeHost != true {
		t.Errorf("expected ExcludeHost true, got %v", h.CacheKey.ExcludeHost)
	}
	if h.CacheKey.ExcludeQueryString == nil || *h.CacheKey.ExcludeQueryString != false {
		t.Errorf("expected ExcludeQueryString false, got %v", h.CacheKey.ExcludeQueryString)
	}
	if len(h.CacheKey.IncludedQueryParams) != 3 || h.CacheKey.IncludedQueryParams[0] != "id" {
		t.Errorf("unexpected IncludedQueryParams: %v", h.CacheKey.IncludedQueryParams)
	}
	if len(h.CacheKey.ExcludedQueryParams) != 1 || h.CacheKey.ExcludedQueryParams[0] != "tracking" {
		t.Errorf("unexpected ExcludedQueryParams: %v", h.CacheKey.ExcludedQueryParams)
	}
	if h.CacheKey.ExcludeMarketingParams == nil || *h.CacheKey.ExcludeMarketingParams != true {
		t.Errorf("expected ExcludeMarketingParams true, got %v", h.CacheKey.ExcludeMarketingParams)
	}
	if len(h.CacheKey.IncludedHeaderNames) != 2 || h.CacheKey.IncludedHeaderNames[0] != "X-App-Version" {
		t.Errorf("unexpected IncludedHeaderNames: %v", h.CacheKey.IncludedHeaderNames)
	}
	if len(h.CacheKey.IncludedCookieNames) != 1 || h.CacheKey.IncludedCookieNames[0] != "session_currency" {
		t.Errorf("unexpected IncludedCookieNames: %v", h.CacheKey.IncludedCookieNames)
	}
	if h.CacheKey.CaseInsensitivePath == nil || *h.CacheKey.CaseInsensitivePath != true {
		t.Errorf("expected CaseInsensitivePath true, got %v", h.CacheKey.CaseInsensitivePath)
	}
	if len(h.CacheKey.IncludedQueryParamValues) != 1 || len(h.CacheKey.IncludedQueryParamValues["format"]) != 2 {
		t.Errorf("unexpected IncludedQueryParamValues: %v", h.CacheKey.IncludedQueryParamValues)
	}
}

func TestCaddyHandler_UnmarshalCaddyfile_ESI(t *testing.T) {
	t.Parallel()
	config := `titip {
		cache_status simple
		storage test
		esi {
			enabled true
			header_required false
			max_depth 3
			max_timeout 15s
			max_concurrent_requests 4
			block_private_ips true
			allowed_hosts cdn.example.com api.partner.com
			max_response_size 5MB
			forward_fragment_cookies true
			preserve_etag true
			error_marker "<!-- error -->"
		}
	}`

	d := caddyfile.NewTestDispenser(config)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("failed to unmarshal caddyfile with esi: %v", err)
	}

	if h.ESI == nil {
		t.Fatalf("expected ESI configuration to be populated")
	}
	if h.ESI.Enabled == nil || !*h.ESI.Enabled {
		t.Errorf("expected ESI.Enabled to be true")
	}
	if h.ESI.PreserveETag == nil || !*h.ESI.PreserveETag {
		t.Errorf("expected PreserveETag true, got %v", h.ESI.PreserveETag)
	}
	if h.ESI.MaxDepth == nil || *h.ESI.MaxDepth != 3 {
		t.Errorf("expected MaxDepth 3, got %v", h.ESI.MaxDepth)
	}
	if h.ESI.MaxTimeout != "15s" {
		t.Errorf("expected MaxTimeout 15s, got %s", h.ESI.MaxTimeout)
	}
	if h.ESI.MaxConcurrentRequests == nil || *h.ESI.MaxConcurrentRequests != 4 {
		t.Errorf("expected MaxConcurrentRequests 4, got %v", h.ESI.MaxConcurrentRequests)
	}
	if len(h.ESI.AllowedHosts) != 2 || h.ESI.AllowedHosts[0] != "cdn.example.com" {
		t.Errorf("unexpected allowed hosts: %v", h.ESI.AllowedHosts)
	}
	if h.ESI.MaxResponseSize != "5MB" {
		t.Errorf("expected MaxResponseSize 5MB, got %s", h.ESI.MaxResponseSize)
	}
	if h.ESI.ErrorMarker != "<!-- error -->" {
		t.Errorf("expected ErrorMarker <!-- error -->, got %s", h.ESI.ErrorMarker)
	}
}

// TestCaddyGlobalOption_Adapt verifies that { titip { ... } } in global options
// compiles properly to apps.titip in Caddy JSON.
func TestCaddyGlobalOption_Adapt(t *testing.T) {
	t.Parallel()

	caddyfileInput := `{
		skip_install_trust
		titip {
			storage test
			cache_status rfc9211
			background_fetch_timeout 5s
			respect_client_cache_control false
			auto_invalidate_mutating_methods true
			esi {
				enabled true
				max_depth 3
				max_timeout 5s
			}
			cache_key {
				include_protocol true
				included_query_params a b
			}
		}
	}
	:8080 {
		route {
			titip
			respond "Hello Global"
		}
	}`

	cadAdapter := caddyconfig.GetAdapter("caddyfile")
	if cadAdapter == nil {
		t.Fatalf("caddyfile adapter not registered")
	}
	jsonBytes, warnings, err := cadAdapter.Adapt([]byte(caddyfileInput), map[string]any{"filename": "Caddyfile"})
	if err != nil {
		t.Fatalf("adapt failed: %v", err)
	}
	for _, w := range warnings {
		t.Logf("warning: %v", w)
	}

	var root map[string]any
	if err := json.Unmarshal(jsonBytes, &root); err != nil {
		t.Fatalf("unmarshal adapted json: %v", err)
	}

	apps, ok := root["apps"].(map[string]any)
	if !ok {
		t.Fatalf("expected apps map in adapted json, got %v", root)
	}

	titipApp, ok := apps["titip"].(map[string]any)
	if !ok {
		t.Fatalf("expected titip app in apps map, got %v", apps)
	}

	storageMap := titipApp["storage"].(map[string]any)
	if storageMap["name"] != "test" {
		t.Errorf("expected storage.name='test', got %v", storageMap["name"])
	}
	if titipApp["cache_status"] != "rfc9211" {
		t.Errorf("expected cache_status rfc9211, got %v", titipApp["cache_status"])
	}
	if titipApp["background_fetch_timeout"] != "5s" {
		t.Errorf("expected background_fetch_timeout 5s, got %v", titipApp["background_fetch_timeout"])
	}
}

// TestCaddyGlobalOption_InheritanceAndOverride tests App provisioning and Handler inheritance via Caddy Context.
func TestCaddyGlobalOption_InheritanceAndOverride(t *testing.T) {
	caddyfileInput := `{
		titip {
			storage test
			cache_status rfc9211
			background_fetch_timeout 10s
			respect_client_cache_control false
			auto_invalidate_mutating_methods true
			esi {
				enabled true
				max_depth 3
			}
		}
	}`

	cadAdapter := caddyconfig.GetAdapter("caddyfile")
	jsonBytes, _, err := cadAdapter.Adapt([]byte(caddyfileInput), map[string]any{"filename": "Caddyfile"})
	if err != nil {
		t.Fatalf("adapt failed: %v", err)
	}

	cfg := new(caddymain.Config)
	if err := json.Unmarshal(jsonBytes, cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}

	ctx, err := caddymain.ProvisionContext(cfg)
	if err != nil {
		t.Fatalf("provision context failed: %v", err)
	}

	// Route handler inheriting global options
	var h Handler
	d := caddyfile.NewTestDispenser("titip")
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal route handler: %v", err)
	}

	if err := h.Provision(ctx); err != nil {
		t.Fatalf("provision route handler: %v", err)
	}
	defer func() { _ = h.Cleanup() }()

	appIface, err := ctx.App("titip")
	if err != nil {
		t.Fatalf("get titip app: %v", err)
	}
	app := appIface.(*App)

	if app.CacheStatus != "rfc9211" {
		t.Errorf("expected global cache_status rfc9211, got %s", app.CacheStatus)
	}
	if app.BackgroundFetchTimeout != "10s" {
		t.Errorf("expected global background_fetch_timeout 10s, got %s", app.BackgroundFetchTimeout)
	}
	if app.RespectClientCacheControl == nil || *app.RespectClientCacheControl != false {
		t.Errorf("expected global respect_client_cache_control false, got %v", app.RespectClientCacheControl)
	}
	if app.AutoInvalidateMutatingMethods == nil || *app.AutoInvalidateMutatingMethods != true {
		t.Errorf("expected global auto_invalidate_mutating_methods true, got %v", app.AutoInvalidateMutatingMethods)
	}
	if app.ESI == nil || app.ESI.MaxDepth == nil || *app.ESI.MaxDepth != 3 {
		t.Errorf("expected global ESI max_depth 3, got %v", app.ESI)
	}
	if h.instance == nil {
		t.Fatalf("expected instance to be provisioned via inherited global storage")
	}
}

// TestDeepMerge_ESIAndCacheKey_Caddyfile verifies that route-level overrides
// merge cleanly on top of global defaults during Caddy Context provisioning.
func TestDeepMerge_ESIAndCacheKey_Caddyfile(t *testing.T) {
	caddyfileInput := `{
		titip {
			storage test
			cache_status rfc9211
			background_fetch_timeout 5s
			esi {
				enabled true
				max_depth 3
				max_timeout 5s
				max_concurrent_requests 8
			}
			cache_key {
				include_protocol true
				included_query_params a b
			}
		}
	}`

	cadAdapter := caddyconfig.GetAdapter("caddyfile")
	jsonBytes, _, err := cadAdapter.Adapt([]byte(caddyfileInput), map[string]any{"filename": "Caddyfile"})
	if err != nil {
		t.Fatalf("adapt failed: %v", err)
	}

	cfg := new(caddymain.Config)
	if err := json.Unmarshal(jsonBytes, cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}

	ctx, err := caddymain.ProvisionContext(cfg)
	if err != nil {
		t.Fatalf("provision context failed: %v", err)
	}

	routeCaddyfile := `titip {
		esi {
			max_depth 10
		}
		cache_key {
			included_query_params a b c
		}
	}`

	var h Handler
	d := caddyfile.NewTestDispenser(routeCaddyfile)
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("unmarshal route handler: %v", err)
	}

	if err := h.Provision(ctx); err != nil {
		t.Fatalf("provision route handler: %v", err)
	}
	defer func() { _ = h.Cleanup() }()

	if h.ESI.MaxDepth == nil || *h.ESI.MaxDepth != 10 {
		t.Errorf("expected route-overridden ESI.MaxDepth=10, got %v", h.ESI.MaxDepth)
	}
	if len(h.CacheKey.IncludedQueryParams) != 3 {
		t.Errorf("expected 3 route-overridden included_query_params, got %v", h.CacheKey.IncludedQueryParams)
	}
	appIface, err := ctx.App("titip")
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	app := appIface.(*App)
	if app.ESI == nil || app.ESI.MaxConcurrentRequests == nil || *app.ESI.MaxConcurrentRequests != 8 {
		t.Errorf("expected inherited global MaxConcurrentRequests=8, got %v", app.ESI)
	}
	if app.CacheKey == nil || app.CacheKey.IncludeProtocol == nil || *app.CacheKey.IncludeProtocol != true {
		t.Errorf("expected inherited global IncludeProtocol=true, got %v", app.CacheKey)
	}
}

// TestCaddyfile_DirectiveOrder_AST verifies that the Caddyfile adapter orders
// titip middleware after encode in the adapted HTTP route handler chain without live TCP listeners.
func TestCaddyfile_DirectiveOrder_AST(t *testing.T) {
	t.Parallel()

	caddyfileInput := `:8080 {
		encode gzip
		titip {
			storage test
		}
		respond "Hello"
	}`

	cadAdapter := caddyconfig.GetAdapter("caddyfile")
	if cadAdapter == nil {
		t.Fatalf("caddyfile adapter not registered")
	}

	jsonBytes, _, err := cadAdapter.Adapt([]byte(caddyfileInput), map[string]any{"filename": "Caddyfile"})
	if err != nil {
		t.Fatalf("adapt failed: %v", err)
	}

	var root map[string]any
	if err := json.Unmarshal(jsonBytes, &root); err != nil {
		t.Fatalf("unmarshal adapted json: %v", err)
	}

	apps, ok := root["apps"].(map[string]any)
	if !ok {
		t.Fatalf("missing apps in adapted json")
	}
	httpApp, ok := apps["http"].(map[string]any)
	if !ok {
		t.Fatalf("missing http app in adapted json")
	}
	servers, ok := httpApp["servers"].(map[string]any)
	if !ok {
		t.Fatalf("missing servers in http app")
	}
	srv0, ok := servers["srv0"].(map[string]any)
	if !ok {
		t.Fatalf("missing srv0 in servers")
	}
	routes, ok := srv0["routes"].([]any)
	if !ok {
		t.Fatalf("missing routes in srv0")
	}

	var handlerOrder []string
	for _, r := range routes {
		routeMap, ok := r.(map[string]any)
		if !ok {
			continue
		}
		if handles, ok := routeMap["handle"].([]any); ok {
			for _, h := range handles {
				if hMap, ok := h.(map[string]any); ok {
					if handlerName, ok := hMap["handler"].(string); ok {
						handlerOrder = append(handlerOrder, handlerName)
					}
				}
			}
		}
	}

	encodeIdx := slices.Index(handlerOrder, "encode")
	titipIdx := slices.Index(handlerOrder, "titip")

	if encodeIdx == -1 {
		t.Fatalf("encode handler not found in adapted routes: %v", handlerOrder)
	}
	if titipIdx == -1 {
		t.Fatalf("titip handler not found in adapted routes: %v", handlerOrder)
	}
	if titipIdx <= encodeIdx {
		t.Errorf("expected titip (idx %d) to be ordered after encode (idx %d), got order: %v", titipIdx, encodeIdx, handlerOrder)
	}
}

func TestCaddyHandler_CacheKey_SmartExcludeQueryStringInheritance(t *testing.T) {
	t.Parallel()

	boolPtr := func(b bool) *bool { return &b }

	// Baseline global config with ExcludeQueryString: true
	globalKey := &CacheKey{
		ExcludeQueryString: boolPtr(true),
	}

	// 1. Route defines IncludedQueryParamValues without ExcludeQueryString -> ExcludeQueryString should become false
	t.Run("RouteWithIncludedQueryParamValues", func(t *testing.T) {
		target := titip.CacheKey{}
		_ = applyCacheKey(&target, globalKey)
		if !target.ExcludeQueryString {
			t.Fatalf("expected global ExcludeQueryString to be true")
		}

		routeKey := &CacheKey{
			IncludedQueryParamValues: map[string][]string{
				"layout": {"marketplace"},
			},
		}
		_ = applyCacheKey(&target, routeKey)
		if target.ExcludeQueryString {
			t.Errorf("expected route allowlist to automatically deactivate ExcludeQueryString, got true")
		}
		if len(target.IncludedQueryParamValues["layout"]) != 1 || target.IncludedQueryParamValues["layout"][0] != "marketplace" {
			t.Errorf("expected IncludedQueryParamValues to be populated, got: %v", target.IncludedQueryParamValues)
		}
	})

	// 2. Route defines IncludedQueryParams without ExcludeQueryString -> ExcludeQueryString should become false
	t.Run("RouteWithIncludedQueryParams", func(t *testing.T) {
		target := titip.CacheKey{}
		_ = applyCacheKey(&target, globalKey)
		if !target.ExcludeQueryString {
			t.Fatalf("expected global ExcludeQueryString to be true")
		}

		routeKey := &CacheKey{
			IncludedQueryParams: []string{"page", "sort"},
		}
		_ = applyCacheKey(&target, routeKey)
		if target.ExcludeQueryString {
			t.Errorf("expected route allowlist to automatically deactivate ExcludeQueryString, got true")
		}
		if len(target.IncludedQueryParams) != 2 {
			t.Errorf("expected 2 IncludedQueryParams, got: %v", target.IncludedQueryParams)
		}
	})

	// 3. Route defines no allowlist and no ExcludeQueryString -> ExcludeQueryString remains true
	t.Run("RouteWithoutAllowlist", func(t *testing.T) {
		target := titip.CacheKey{}
		_ = applyCacheKey(&target, globalKey)

		routeKey := &CacheKey{
			CaseInsensitivePath: boolPtr(true),
		}
		_ = applyCacheKey(&target, routeKey)
		if !target.ExcludeQueryString {
			t.Errorf("expected ExcludeQueryString to remain true when route has no allowlist, got false")
		}
		if !target.CaseInsensitivePath {
			t.Errorf("expected CaseInsensitivePath to be true, got false")
		}
	})

	// 4. Route defines allowlist but explicitly sets ExcludeQueryString: true -> Explicit choice honored
	t.Run("RouteExplicitExcludeQueryStringTrue", func(t *testing.T) {
		target := titip.CacheKey{}
		_ = applyCacheKey(&target, globalKey)

		routeKey := &CacheKey{
			ExcludeQueryString: boolPtr(true),
			IncludedQueryParamValues: map[string][]string{
				"layout": {"marketplace"},
			},
		}
		_ = applyCacheKey(&target, routeKey)
		if !target.ExcludeQueryString {
			t.Errorf("expected explicit ExcludeQueryString=true on route to take precedence, got false")
		}
	})

	// 5. Route defines allowlist and explicitly sets ExcludeQueryString: false -> Explicit choice honored
	t.Run("RouteExplicitExcludeQueryStringFalse", func(t *testing.T) {
		target := titip.CacheKey{}
		_ = applyCacheKey(&target, globalKey)

		routeKey := &CacheKey{
			ExcludeQueryString:  boolPtr(false),
			IncludedQueryParams: []string{"page"},
		}
		_ = applyCacheKey(&target, routeKey)
		if target.ExcludeQueryString {
			t.Errorf("expected explicit ExcludeQueryString=false on route to take precedence, got true")
		}
	})
}

func TestCaddyHandler_ServerTiming_UnmarshalCaddyfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   string
		validate func(t *testing.T, h *Handler)
	}{
		{
			name: "server_timing simple",
			config: `titip {
				server_timing
				storage test
			}`,
			validate: func(t *testing.T, h *Handler) {
				if h.ServerTiming == nil || h.ServerTiming.Enabled == nil || !*h.ServerTiming.Enabled {
					t.Fatalf("expected ServerTiming enabled, got: %v", h.ServerTiming)
				}
				if h.ServerTiming.CookieName != "" {
					t.Fatalf("expected empty cookie name, got: %s", h.ServerTiming.CookieName)
				}
			},
		},
		{
			name: "server_timing false",
			config: `titip {
				server_timing false
				storage test
			}`,
			validate: func(t *testing.T, h *Handler) {
				if h.ServerTiming == nil || h.ServerTiming.Enabled == nil || *h.ServerTiming.Enabled {
					t.Fatalf("expected ServerTiming disabled, got: %v", h.ServerTiming)
				}
			},
		},
		{
			name: "server_timing cookie",
			config: `titip {
				server_timing cookie debug_cookie secret_val
				storage test
			}`,
			validate: func(t *testing.T, h *Handler) {
				if h.ServerTiming == nil || h.ServerTiming.Enabled == nil || !*h.ServerTiming.Enabled {
					t.Fatalf("expected ServerTiming enabled, got: %v", h.ServerTiming)
				}
				if h.ServerTiming.CookieName != "debug_cookie" {
					t.Fatalf("expected cookie name 'debug_cookie', got: %s", h.ServerTiming.CookieName)
				}
				if h.ServerTiming.CookieValue != "secret_val" {
					t.Fatalf("expected cookie value 'secret_val', got: %s", h.ServerTiming.CookieValue)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := caddyfile.NewTestDispenser(tc.config)
			var h Handler
			if err := h.UnmarshalCaddyfile(d); err != nil {
				t.Fatalf("unmarshal error: %v", err)
			}
			tc.validate(t, &h)
		})
	}
}
