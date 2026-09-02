import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom k6 metrics for Titip caching performance
const cacheHits = new Counter('titip_cache_hits');
const cacheMisses = new Counter('titip_cache_misses');
const cacheRevalidated = new Counter('titip_cache_revalidated');
const cacheHitRate = new Rate('titip_cache_hit_rate');

const hitDuration = new Trend('titip_hit_duration', true);
const missDuration = new Trend('titip_miss_duration', true);
const esiDuration = new Trend('titip_esi_duration', true);

// Configurable environment options
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const ADMIN_URL = __ENV.ADMIN_URL || 'http://localhost:2019';

export const options = {
    scenarios: {
        // High-concurrency traffic simulating realistic caching & ESI workload
        caching_traffic: {
            executor: 'ramping-vus',
            startVUs: 5,
            stages: [
                { duration: '5s', target: 25 },  // Ramp-up
                { duration: '20s', target: 50 }, // Sustained load
                { duration: '5s', target: 0 },   // Ramp-down
            ],
            gracefulRampDown: '2s',
        },
    },
    thresholds: {
        http_req_failed: ['rate<0.01'],             // Less than 1% errors
        'titip_cache_hit_rate': ['rate>0.80'],       // Expect > 80% cache hit rate overall
        'titip_hit_duration': ['p(95)<25'],         // 95% of cache hits should resolve under 25ms under 100 VUs
    },
};

const LANGUAGES = ['en-US', 'id-ID', 'ja-JP', 'de-DE', 'fr-FR'];

export default function () {
    const rand = Math.random();

    // 1. Time API (High frequency, cached 30s + stale-while-revalidate) [40% traffic]
    if (rand < 0.40) {
        const res = http.get(`${BASE_URL}/api/time`);
        trackCacheStatus(res);
        check(res, {
            'time status 200': (r) => r.status === 200,
            'time has Cache-Status': (r) => r.headers['Cache-Status'] !== undefined,
        });
    }
    // 2. Products API (Catalog cached with surrogate tags) [30% traffic]
    else if (rand < 0.70) {
        const res = http.get(`${BASE_URL}/api/products`);
        trackCacheStatus(res);
        check(res, {
            'products status 200': (r) => r.status === 200,
            'products body valid': (r) => typeof r.body === 'string' && r.body.includes('Cloud Edge CDN'),
        });
    }
    // 3. Multi-Variant Language API (Tests RFC Vary negotiation) [15% traffic]
    else if (rand < 0.85) {
        const lang = LANGUAGES[Math.floor(Math.random() * LANGUAGES.length)];
        const res = http.get(`${BASE_URL}/api/vary`, {
            headers: { 'Accept-Language': lang },
        });
        trackCacheStatus(res);
        check(res, {
            'vary status 200': (r) => r.status === 200,
            'vary language matched': (r) => typeof r.body === 'string' && r.body.includes(lang),
        });
    }
    // 4. Edge Side Includes (ESI) composite page [14% traffic]
    else if (rand < 0.99) {
        const start = Date.now();
        const res = http.get(`${BASE_URL}/esi-demo`);
        esiDuration.add(Date.now() - start);

        check(res, {
            'esi status 200': (r) => r.status === 200,
            'esi spliced static fragment': (r) => typeof r.body === 'string' && r.body.includes('Caddy Static Fragment'),
            'esi spliced header': (r) => typeof r.body === 'string' && r.body.includes('Global ESI Header Component'),
            'esi spliced clock': (r) => typeof r.body === 'string' && r.body.includes('Live Dynamic Clock Fragment'),
        });
    }
    // 5. Cache Invalidation / Purge test [1% traffic]
    else {
        const purgeType = Math.random() > 0.5 ? 'tag' : 'url';
        let payload;
        if (purgeType === 'tag') {
            payload = JSON.stringify({ tags: ['products'], soft: true });
        } else {
            payload = JSON.stringify({ urls: ['http://localhost:8080/api/time'], soft: true });
        }

        const res = http.post(`${BASE_URL}/api/purge`, payload, {
            headers: { 'Content-Type': 'application/json' },
        });
        check(res, {
            'purge status 200': (r) => r.status === 200,
        });
    }

    // Optional think time for realistic simulation (disabled by default for maximum throughput)
    if (__ENV.THINK_TIME === 'true') {
        sleep(0.005 + Math.random() * 0.02);
    }
}

// Helper to inspect RFC 9211 Cache-Status header and populate custom k6 trends
function trackCacheStatus(res) {
    const statusHeader = res.headers['Cache-Status'] || '';
    const isHit = statusHeader.includes('hit') || statusHeader === 'HIT';
    const isReval = statusHeader.includes('stale-while-revalidate') || statusHeader.includes('304');

    if (isHit) {
        cacheHits.add(1);
        cacheHitRate.add(true);
        hitDuration.add(res.timings.duration);
    } else {
        cacheMisses.add(1);
        cacheHitRate.add(false);
        missDuration.add(res.timings.duration);
    }

    if (isReval) {
        cacheRevalidated.add(1);
    }
}

// Formatted summary displayed after load test finishes
export function handleSummary(data) {
    return {
        stdout: textSummary(data),
    };
}

function textSummary(data) {
    const hits = data.metrics.titip_cache_hits ? data.metrics.titip_cache_hits.values.count : 0;
    const misses = data.metrics.titip_cache_misses ? data.metrics.titip_cache_misses.values.count : 0;
    const total = hits + misses;
    const hitRate = total > 0 ? ((hits / total) * 100).toFixed(2) : 0;

    const hitP95 = data.metrics.titip_hit_duration ? data.metrics.titip_hit_duration.values['p(95)'].toFixed(2) : 'N/A';
    const missP95 = data.metrics.titip_miss_duration ? data.metrics.titip_miss_duration.values['p(95)'].toFixed(2) : 'N/A';
    const esiP95 = data.metrics.titip_esi_duration ? data.metrics.titip_esi_duration.values['p(95)'].toFixed(2) : 'N/A';
    const reqTotal = data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0;
    const rps = data.metrics.http_reqs ? data.metrics.http_reqs.values.rate.toFixed(1) : 0;

    return `
================================================================================
🚀 TITIP CADDY DEMO — LOAD TEST RESULTS
================================================================================
Requests Total       : ${reqTotal} (${rps} req/s)
Cache Hits           : ${hits}
Cache Misses         : ${misses}
Cache Hit Rate       : ${hitRate}%

Latency (p95):
  • Cache Hit        : ${hitP95} ms
  • Cache Miss       : ${missP95} ms
  • ESI Composite    : ${esiP95} ms

--------------------------------------------------------------------------------
💡 View Caddy Prometheus metrics:
   curl http://localhost:2019/metrics | grep titip_
================================================================================
`;
}
