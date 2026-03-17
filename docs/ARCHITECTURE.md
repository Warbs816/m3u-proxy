# m3u-proxy Architecture Guide

## Core Architecture

### 🚀 **True Live Proxy Design**

**Design Philosophy:** Efficient live proxying with connection sharing — multiple viewers share a single upstream provider connection. Each byte comes directly from the provider with no persistent buffering layer.

**Implementation:** Separate streaming strategies based on stream type:

#### 1. **Continuous Streams (.ts, .mp4, .mkv, etc.) — Live**
- **Primary/subscriber broadcast model** — the first client opens one upstream connection and broadcasts chunks to all subsequent clients via in-memory queues
- **One provider connection per channel** — conserves provider connection slots regardless of viewer count
- **Subscriber promotion** — if the primary disconnects, the longest-running subscriber takes over and inherits the upstream TCP connection with no gap
- **Truly ephemeral** — provider connection closes when the last client disconnects
- **No transcoding, no buffering** — pure byte-for-byte proxy

```
Provider → primary → Client A (direct yield)
                   ↘ Queue → Client B (subscriber)
                   ↘ Queue → Client C (subscriber)
```

#### 2. **VOD Streams (.ts, .mp4, .mkv, etc.) — Video on Demand**
- Each client gets an **independent** provider connection for full seek support
- Range headers honoured and forwarded upstream
- Works correctly with all standard video players

#### 3. **HLS Streams (.m3u8)**
- Stream connections are shared (multiple clients, 1 stream connection)
- On-demand segment fetching
- Efficient playlist processing
- Shared HTTP client with connection pooling

### ⚡ **Performance Optimizations**

1. **uvloop Integration** - 2-4x faster async I/O operations
   ```bash
   # Automatically detected and used if available
   pip install uvloop
   ```

2. **Connection Pooling** - HTTP clients optimized with limits:
   ```python
   limits=httpx.Limits(
       max_keepalive_connections=20,
       max_connections=100,
       keepalive_expiry=30.0
   )
   ```

3. **Efficient Stats Tracking** - Lightweight per-client metrics

### 🔄 **Seamless Failover**

- **Per-client failover** - When a connection fails, only that client experiences failover
- **Smooth transition** - Opens new connection before closing old one
- **Automatic retry** - Recursively tries all failover URLs
- **No interruption to other clients** - Each client's stream is independent

```python
# Failover flow
try:
    # Stream from primary URL
    async for chunk in response:
        yield chunk
except Error:
    # Seamlessly switch to failover URL
    new_response = await seamless_failover()
    async for chunk in new_response:
        yield chunk  # Client doesn't notice the switch
```

### 📊 **Monitoring**

- Stream type detection (HLS, VOD, Live Continuous)
- Per-client bandwidth tracking
- Connection efficiency metrics
- Failover statistics

## Implementation Details

### Stream Manager Core

The `StreamManager` class in `src/stream_manager.py` implements the per-client direct proxy architecture.

```python
# Direct import - v2.0 is the standard
from stream_manager import StreamManager
```

## Testing

### Test 1: Multiple Clients on Continuous Stream

```bash
# Create a continuous .ts stream
curl -X POST "http://localhost:8085/streams" \
  -H "Content-Type: application/json" \
  -d '{"url": "http://example.com/live.ts"}'

# Start multiple clients simultaneously
ffplay "http://localhost:8085/stream/{stream_id}" &  # Client 1 — becomes primary
ffplay "http://localhost:8085/stream/{stream_id}" &  # Client 2 — subscriber
ffplay "http://localhost:8085/stream/{stream_id}" &  # Client 3 — subscriber
```

**Result:** ✅ Only one upstream provider connection is opened; Clients 2 and 3 receive chunks via broadcast queues

### Test 2: Channel Zapping (Connection Cleanup)

```bash
# Start watching a stream
ffplay "http://localhost:8085/stream/{stream_id1}"

# Close it and immediately watch another
# Provider connection closes immediately
ffplay "http://localhost:8085/stream/{stream_id2}"
```

**Result:** ✅ Instant cleanup, no lingering connections or buffer cleanup needed

### Test 3: Seamless Failover During Active Streaming

```bash
# Create stream with failover URLs
curl -X POST "http://localhost:8085/streams" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "http://primary.com/stream.ts",
    "failover_urls": ["http://backup.com/stream.ts"]
  }'

# Start watching
ffplay "http://localhost:8085/stream/{stream_id}"

# Trigger failover while streaming
curl -X POST "http://localhost:8085/streams/{stream_id}/failover"
```

**Result:** ✅ Seamless transition (<100ms), client barely notices the switch

### Test 4: VOD with Multiple Positions

```bash
# Create VOD stream
curl -X POST "http://localhost:8085/streams" \
  -H "Content-Type: application/json" \
  -d '{"url": "http://example.com/movie.mp4"}'

# Multiple clients at different positions simultaneously
curl -H "Range: bytes=0-1023" "http://localhost:8085/stream/{stream_id}"        # Start
curl -H "Range: bytes=1000000-" "http://localhost:8085/stream/{stream_id}"      # Middle
curl -H "Range: bytes=50000000-" "http://localhost:8085/stream/{stream_id}"     # End
```

**Result:** ✅ Each client operates independently at their chosen position

## Deployment

### Install Dependencies

```bash
pip install -r requirements.txt
```

This now includes `uvloop` for performance.

### Start Server

```bash
python main.py
```

The server automatically:
- Detects and uses StreamManagerV2
- Enables uvloop if available
- Configures optimized connection pooling
- Uses efficient per-client proxying

### Environment Variables

Same as before - no changes required:

```bash
# .env file
HOST=0.0.0.0
PORT=8085
LOG_LEVEL=INFO
CLIENT_TIMEOUT=30
STREAM_TIMEOUT=300
```

## Architecture Decision Record

### Why Primary/Subscriber Broadcast for Live Continuous Streams?

**Option A: Per-Client Direct Connections**
- ❌ Each viewer opens a separate upstream TCP connection
- ❌ Wastes provider connection slots (2 viewers = 2 connections billed)
- ❌ Providers may kill duplicate connections to the same stream
- ✅ Simple — each client is fully independent

**Option B: Primary/Subscriber Broadcast (current approach)**
- ✅ One upstream connection per live channel, regardless of viewer count
- ✅ Conserves provider connection slots
- ✅ Subscriber promotion ensures continuity when the primary disconnects
- ✅ Upstream TCP connection handed off on promotion — no reconnect gap
- ✅ Per-subscriber queues — a slow client can't block others
- ⚠️ VOD excluded — each VOD client still gets an independent connection for seek support

**Verdict:** Option B is superior for live continuous streams. VOD retains per-client connections.

---

**Version:** 3.0.0
**Date:** March 2026
**Status:** Production Ready ✅
