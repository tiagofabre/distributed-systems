# Jetty re-evaluation

Analysis of Jetty's role in the KV node stack (Javalin on Jetty 11), based on CPU flamegraph and code, and options to tune or replace it.

---

## Current stack

- **Javalin 6.1.3** embeds **Jetty 11.0.20**.
- The app uses **virtual threads** (`javalinConfig.useVirtualThreads = true`): each request runs on a virtual thread; Jetty's `ManagedSelector` and NIO run on platform threads and dispatch to a `ThreadPerTaskExecutor` that runs the handler on a virtual thread.
- Flow: `kevent` / `SelectorImpl.select` -> `ManagedSelector` -> `SelectableChannelEndPoint` -> `FillInterest` -> `HttpConnection.onFillable` -> read/parse -> `HttpChannel.handle` -> Servlet stack -> `JavalinServlet` -> our handlers.

---

## What the CPU profile shows

From `build/cpu-collapsed.txt` under PUT load:

1. **I/O and selector**
   - `ManagedSelector.nioSelect` -> `KQueueSelectorImpl.doSelect` -> `kevent`: time waiting for or processing socket events (macOS kqueue). Expected for a network server.

2. **Jetty request path**
   - `HttpConnection.onFillable` -> `HttpChannel.handle` -> `HttpChannel.dispatch` -> ... -> `JavalinJettyServlet.service` -> `JavalinServlet.handle` -> `KvJavalinHandler.put`.
   - `HttpConnection.parseRequestBuffer` -> `HttpParser.parseNext` / `parseFields`: HTTP/1.1 parsing (request line, headers).
   - `HttpConnection.fillRequestBuffer` -> `SocketChannelEndPoint.fill` -> `read`: reading body from socket.
   - `HttpOutput.channelWrite` / `WriteFlusher` / `sendResponse`: writing response.

3. **Virtual threads**
   - `VirtualThread.runContinuation` and ForkJoinPool: scheduling of virtual threads. Overhead is modest compared to the rest.

4. **Application code**
   - After the optimizations (streaming PUT parse, replica list view, getNodeCount), the remaining app cost is in handler logic, replication client, and store/clock. Jetty still appears as the container that does I/O and HTTP parsing.

So: a large share of samples are in **Jetty I/O and HTTP parsing** and in **kernel/selector (kevent)**. That is normal for an HTTP server; the question is whether we can tune Jetty or reduce cost by changing the server.

---

## Options

### 1. Keep Jetty and tune it (recommended first step)

Javalin exposes Jetty configuration without replacing the server:

- **`config.jetty.modifyHttpConfiguration(httpConfig -> { ... })`**  
  Use this to set:
  - **Request header size**  
    `httpConfig.setRequestHeaderSize(int)`: max size of request line + headers. Our API uses short URLs and small headers; 8 KiB is plenty. Defaults in Jetty are often 8 KiB; keeping them explicit avoids surprises and allows lowering if we ever want to constrain resource use.
  - **Output buffer**  
    `httpConfig.setOutputBufferSize(int)`: buffer used for response writing. Small JSON responses (e.g. GET) or 204 PUT fit in a small buffer; larger buffer can reduce syscalls at the cost of memory per connection.
  - **Other**  
    `setResponseHeaderSize`, timeouts, etc. can be adjusted if needed.

- **`config.jetty.modifyServer(server -> { ... })`**  
  For more invasive changes (e.g. connector-level settings, thread pools). Usually not needed if we only care about buffer sizes.

No code change is strictly required for correctness; tuning is optional and should be validated with a new flamegraph and load test after applying it.

### 2. Replace Jetty with another server

- **Netty**  
  Javalin does not ship a Netty backend. Using Netty would mean either:
  - Writing a small HTTP server with Netty and our own routing (and servlet-like request/response abstraction), or
  - Using another framework that runs on Netty (e.g. Spring WebFlux, Micronaut, or a minimal Netty-based router).  
  That is a large change (rewrite of HTTP layer, different concurrency model unless we still use virtual threads on top of Netty’s event loop). Only justified if profiling shows that Jetty’s HTTP parsing or I/O are a proven bottleneck and we have evidence that Netty would improve it (e.g. fewer allocations, zero-copy, or different buffer management).

- **Other embedded servers**  
  Javalin is built around Jetty; switching to another server means leaving Javalin or using a different framework. Not recommended unless we are ready for a full stack change.

### 3. Do nothing

Jetty 11 is mature, supports virtual threads well with Javalin, and the profile shows expected work (I/O, HTTP parsing, handler dispatch). If throughput and latency already meet targets, we can keep the current stack and only revisit if new profiles or requirements justify it.

---

## Recommendation

1. **Short term:** Add optional Jetty tuning via `config.jetty.modifyHttpConfiguration` (e.g. explicit `setRequestHeaderSize(8192)`, and optionally `setOutputBufferSize` after checking Jetty defaults). Re-profile with `make flamegraph-run` to see if there is any visible change.
2. **Medium term:** If the flamegraph continues to show Jetty/HTTP parsing as a large fraction of CPU and we have clear performance targets we are missing, consider a dedicated experiment (e.g. a minimal Netty-based endpoint) to compare cost per request.
3. **Otherwise:** Keep Jetty; focus optimization on application logic (already done for PUT parsing and replica list) and replication path.

---

## Applied tuning

**No longer applicable:** The application no longer uses Jetty. It was replaced by Netty (see `docs/http-server-migration-plan.md`).

Previously, in `KvNodeApplication`, Jetty's `HttpConfiguration` was tuned via Javalin's `config.jetty.modifyHttpConfiguration` (request header size 8 KiB). That code was removed with the Javalin/Jetty removal.

## References

- Javalin docs: Server setup, `config.jetty.modifyHttpConfiguration`, `config.jetty.modifyServer`.
- Jetty 11 `HttpConfiguration`: request/response header size, output buffer size, connection idle timeouts.
- Project: `docs/flamegraph.md`, `docs/performance-optimizations.md`.
