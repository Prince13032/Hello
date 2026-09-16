// worker.js
var ORIGIN_HOST = "origin-gcp.wy-code.tech";
var ORIGIN_PUBLIC_HOST = "gcp.wy-code.tech";

// === WebSocket upstream (ADDED) ===
// The CCR agent-proxy dials wss://gcp.wy-code.tech/v1/code/agent-proxy/ws.
// The HTTP origin (Google Frontend) answers 404 on that path and cannot do
// WebSocket through this worker, so WS upgrades are tunneled straight to
// Anthropic instead.
var WS_UPSTREAM_HOST = "api.anthropic.com";
// Path prefixes routed to WS_UPSTREAM_HOST on Upgrade: websocket.
// Narrow to ["/v1/code/agent-proxy/"] for minimum surface.
var WS_PATH_PREFIXES = ["/v1/code/"];

var CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, Accept",
  "Access-Control-Max-Age": "86400"
};

// Hop-by-hop / CF-injected headers that must never be forwarded upstream.
var WS_STRIP_HEADERS = [
  "host", "connection", "upgrade",
  "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions",
  "accept-encoding",
  "cf-connecting-ip", "cf-ipcountry", "cf-ray", "cf-visitor", "cf-worker",
  "x-forwarded-for", "x-forwarded-proto", "x-real-ip"
];

function isWebSocketRequest(request) {
  return (request.headers.get("Upgrade") || "").toLowerCase() === "websocket";
}

// Bridge a client WebSocket <-> upstream WebSocket (api.anthropic.com).
async function handleWebSocket(request, url) {
  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);

  // Forward auth/api headers (Authorization, anthropic-*, user-agent, ...).
  const upstreamHeaders = new Headers();
  for (const [k, v] of request.headers) {
    if (WS_STRIP_HEADERS.includes(k.toLowerCase())) continue;
    upstreamHeaders.set(k, v);
  }
  // Preserve subprotocol offer (some clients carry auth here).
  const subproto = request.headers.get("Sec-WebSocket-Protocol");
  if (subproto) upstreamHeaders.set("Sec-WebSocket-Protocol", subproto);
  upstreamHeaders.set("Upgrade", "websocket");

  const upstreamUrl = "https://" + WS_UPSTREAM_HOST + url.pathname + url.search;
  let upstreamResp;
  try {
    upstreamResp = await fetch(upstreamUrl, { headers: upstreamHeaders });
  } catch (err) {
    return new Response(
      JSON.stringify({ error: "WS upstream unreachable", host: WS_UPSTREAM_HOST, error: String(err) }),
      { status: 502, headers: { "Content-Type": "application/json", ...CORS_HEADERS } }
    );
  }
  const upstream = upstreamResp.webSocket;
  if (!upstream) {
    // Upstream refused the upgrade — surface its status so the client
    // (and /__agentproxy/status recentRelayFailures) shows the real cause.
    const body = await upstreamResp.text().catch(() => "");
    return new Response(
      JSON.stringify({ error: "WS upstream refused upgrade", status: upstreamResp.status, body: body.slice(0, 500) }),
      { status: 502, headers: { "Content-Type": "application/json", ...CORS_HEADERS } }
    );
  }

  upstream.accept();
  server.accept();

  server.addEventListener("message", (e) => { try { upstream.send(e.data); } catch (_) {} });
  upstream.addEventListener("message", (e) => { try { server.send(e.data); } catch (_) {} });
  server.addEventListener("close", (e) => { try { upstream.close(e.code, e.reason); } catch (_) {} });
  upstream.addEventListener("close", (e) => { try { server.close(e.code, e.reason); } catch (_) {} });
  server.addEventListener("error", () => { try { upstream.close(1011); } catch (_) {} });
  upstream.addEventListener("error", () => { try { server.close(1011); } catch (_) {} });

  return new Response(null, { status: 101, webSocket: client });
}

var worker_default = {
  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname + url.search;

    // === WebSocket routing (ADDED) ===
    if (isWebSocketRequest(request)) {
      if (WS_PATH_PREFIXES.some((p) => url.pathname.startsWith(p))) {
        return handleWebSocket(request, url);
      }
      return new Response("WebSocket not supported on this path", { status: 404 });
    }

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }
    const reqHeaders = new Headers(request.headers);
    reqHeaders.set("Host", ORIGIN_PUBLIC_HOST);
    reqHeaders.set("Accept-Encoding", "identity");
    for (const h of ["cf-connecting-ip", "cf-ipcountry", "cf-ray", "cf-visitor", "cf-worker", "x-forwarded-for", "x-forwarded-proto", "x-real-ip"]) {
      reqHeaders.delete(h);
    }
    const init = {
      method: request.method,
      headers: reqHeaders,
      redirect: "manual",
      cf: { cacheTtl: 0, cacheEverything: false }
    };
    if (!["GET", "HEAD"].includes(request.method)) {
      init.body = request.body;
    }
    const t0 = Date.now();
    const originUrl = "https://" + ORIGIN_PUBLIC_HOST + path;
    init.cf = { ...init.cf, resolveOverride: ORIGIN_HOST };
    let response;
    let usedProto = "https";
    try {
      response = await fetch(originUrl, init);
    } catch (err) {
      return new Response(
        JSON.stringify({ error: "Origin unreachable", host: ORIGIN_HOST, error: String(err) }),
        { status: 502, headers: { "Content-Type": "application/json", ...CORS_HEADERS } }
      );
    }
    const ttfb = Date.now() - t0;
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      const location = response.headers.get("location");
      if (location) {
        const locUrl = new URL(location, "https://" + url.hostname);
        const headers2 = new Headers(response.headers);
        headers2.set("location", locUrl.href);
        Object.entries(CORS_HEADERS).forEach(([k, v]) => headers2.set(k, v));
        return new Response(null, { status: response.status, headers: headers2 });
      }
    }
    const contentType = response.headers.get("content-type") || "";
    const isStreaming = contentType.includes("text/event-stream") || contentType.includes("x-ndjson") || contentType.includes("x-json-stream");
    const headers = new Headers(response.headers);
    headers.set("X-Accel-Buffering", "no");
    headers.set("Cache-Control", "no-cache, no-store, must-revalidate");
    headers.set("Connection", "keep-alive");
    headers.set("X-Content-Type-Options", "nosniff");
    headers.set("Server-Timing", "origin-ttfb;dur=" + ttfb + ";proto=" + usedProto);
    headers.set("X-Origin-Protocol", usedProto);
    Object.entries(CORS_HEADERS).forEach(([k, v]) => headers.set(k, v));
    if (isStreaming && response.body) {
      const { readable, writable } = new TransformStream({
        transform(chunk, controller) {
          controller.enqueue(chunk);
        },
        // Send keep-alive comment every 15s of silence to prevent proxy timeout
        start(controller) {
          let lastData = Date.now();
          const interval = setInterval(() => {
            if (Date.now() - lastData > 14e3) {
              controller.enqueue(new TextEncoder().encode(": keep-alive\n\n"));
              lastData = Date.now();
            }
          }, 5e3);
          this._interval = interval;
        },
        cancel() {
          if (this._interval) clearInterval(this._interval);
        }
      });
      response.body.pipeTo(writable).catch(() => {
      });
      return new Response(readable, { status: response.status, headers });
    }
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers
    });
  }
};
export {
  worker_default as default
};
//# sourceMappingURL=worker.js.map
