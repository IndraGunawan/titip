# ESI (Edge Side Includes)

Package `github.com/indragunawan/titip/esi` provides an [Edge Side Includes (ESI 1.0)](https://www.w3.org/TR/esi-lang/) processor for Go.

## Features

- **Buffer Pooling**: Uses `sync.Pool` for buffer reuse during document splicing.
- **Concurrent Fetching**: Fetches fragment targets concurrently up to a configured limit.
- **SSRF Protection**: Blocks private, loopback, and link-local IP ranges by default for outbound HTTP includes.
- **In-Process Fetching**: Resolves local paths via `http.Handler` before falling back to outbound HTTP.
- **Recursion Limits & Cycle Detection**: Prevents infinite loops and limits nesting depth.
- **Prometheus Metrics**: Exports fragment counts and latency distributions.

## Supported Directives

| Directive | Syntax | Description |
| :--- | :--- | :--- |
| **Include** | `<esi:include src="..." />` | Fetches and inlines fragment content. |
| **Include with Fallback** | `<esi:include src="..." alt="..." />` | Fetches `alt` URL if `src` fails. |
| **Paired Include** | `<esi:include src="...">Fallback Content</esi:include>` | Renders enclosed content if `src` and `alt` fail. |
| **Continue on Error** | `<esi:include src="..." onerror="continue" />` | Suppresses errors and omits fragment if fetch fails. |
| **Remove** | `<esi:remove>...</esi:remove>` | Strips enclosed content when ESI processing is active. |
| **Comment** | `<esi:comment text="..." />` | Stripped from output. |
| **Comment Wrapper** | `<!--esi ... -->` | Unescapes enclosed content during ESI processing. |

## Quickstart

### Standalone Document Processing

```go
package main

import (
    "context"
    "fmt"
    "net/http"

    "github.com/indragunawan/titip/esi"
)

func main() {
    proc := esi.NewProcessor()

    html := []byte(`
        <html>
            <body>
                <header><esi:include src="https://example.com/header" /></header>
                <main>Main Content</main>
                <!--esi <footer>Rendered by ESI</footer> -->
            </body>
        </html>
    `)

    req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com/", nil)

    result, err := proc.ProcessDocument(context.Background(), req, html)
    if err != nil {
        panic(err)
    }
    defer result.Release()

    fmt.Println(string(result.Body()))
}
```

### In-Process Subrequests (`HandlerFetcher`)

Resolve local paths directly through an `http.Handler` instead of outbound HTTP:

```go
mux := http.NewServeMux()
mux.HandleFunc("/fragments/user", func(w http.ResponseWriter, r *http.Request) {
    w.Write([]byte(`<span>Logged in as Alice</span>`))
})

proc := esi.NewProcessor(
    esi.WithInternalFetcher(esi.HandlerFetcher(mux)),
)
```

If the handler returns 404, the processor falls back to outbound HTTP.

## Configuration Options

| Option | Default | Description |
| :--- | :--- | :--- |
| `WithInternalFetcher(fn)` | `nil` | In-process subrequest handler. Use `esi.HandlerFetcher(router)` to adapt an `http.Handler`. |
| `WithHeaderRequired(bool)` | `false` | When true, documents are only processed if `Surrogate-Control` contains `ESI/1.0`. |
| `WithMaxDepth(uint32)` | `3` | Maximum nesting depth for recursive includes. |
| `WithMaxTimeout(time.Duration)` | `30s` | Timeout per fragment request. |
| `WithMaxConcurrentRequests(int)` | `8` | Maximum concurrent fragment requests per document. |
| `WithMaxResponseSize(int64)` | `10MB` | Maximum fragment response size in bytes. |
| `WithAllowPrivateIPs(bool)` | `false` | When true, allows requests to private, loopback, and link-local IP addresses. |
| `WithAllowedHosts(...string)` | `[]` | Allowed hostnames for outbound requests (empty allows any public host). |
| `WithAllowPrivateIPsForAllowedHosts(bool)` | `false` | Allows private IPs for explicitly allowed hosts. |
| `WithDisableForwardCookies(bool)` | `false` | Prevents forwarding `Set-Cookie` headers from fragment responses. |
| `WithIncludeErrorMarker(string)` | `""` | HTML placeholder rendered when an include fails without fallback. |
| `WithPreserveETag(bool)` | `false` | Preserves downstream `ETag` (weakened) and `Last-Modified` headers. |
| `WithMetrics(reg)` | `nil` | Prometheus registerer for fragment metrics. |
| `WithLogger(logger)` | `slog.Default()` | Logger instance (`*slog.Logger`). |
| `WithHTTPClient(client)` | SSRF-safe client | Custom `*http.Client` for outbound requests. |

## Protocol Helpers

Helpers for upstream capability negotiation defined in the [W3C ESI 1.0 / Edge Architecture Specification](https://www.w3.org/TR/esi-lang/):

- `esi.AddSurrogateCapability(header http.Header, deviceID string)`: Advertises `Surrogate-Capability: <deviceID>="ESI/1.0"` to upstream origin servers.
- `esi.HasSurrogateCapability(header http.Header, deviceID string) bool`: Checks for an ESI/1.0 capability token.
- `esi.HasSurrogateControl(header http.Header) bool`: Checks if `Surrogate-Control` contains `ESI/1.0`.
- `proc.IsEligible(header http.Header) bool`: Checks if response headers meet ESI processing requirements based on `WithHeaderRequired`.

## Memory Management

Call `result.Release()` after reading `result.Body()` to return the buffer to the pool:

```go
result, err := proc.ProcessDocument(ctx, req, body)
if err != nil {
    return err
}
defer result.Release()

w.Write(result.Body())
```
