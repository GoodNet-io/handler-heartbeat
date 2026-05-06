# Contract: Heartbeat — `gn.heartbeat` Extension

**Status:** active · v1
**Owner:** `plugins/handlers/heartbeat/`
**SDK header:** `sdk/extensions/heartbeat.h`
**Stability:** stable for v1.x; future minor versions append vtable
slots through size-prefix evolution per `docs/contracts/abi-evolution.md`.

---

## 1. Purpose

Two-way liveness check between connected peers. The handler emits
PING envelopes on demand, replies to received PINGs with PONGs that
echo the requester's timestamp plus the requester's apparent
endpoint as observed from the responder's side, and exposes two
derived signals through the `gn.heartbeat` extension:

- per-connection round-trip time, sourced from the latest PONG;
- per-connection observed external address — the peer's view of
  this node's endpoint, reflected back in PONG. This is
  STUN-on-the-wire: no STUN server, no separate UDP traffic, the
  observation rides the same encrypted frame as application data.

The handler does not own a timer. An application plugin or a
test harness drives `send_ping(conn)` on its own cadence; the
default policy decision (interval, miss tolerance) belongs to the
caller, not to this contract.

---

## 2. Wire format

A heartbeat envelope is a regular `gn_message_t` carried by the
active protocol layer with `msg_id == 0x10`. The payload is exactly
**88 bytes**, big-endian for every multi-byte integer:

| Offset | Size | Field           | Notes |
|--------|------|-----------------|-------|
| 0      | 8    | `timestamp_us`  | monotonic clock at send |
| 8      | 4    | `seq`           | per-peer monotonic counter |
| 12     | 1    | `flags`         | `0x00` PING, `0x01` PONG |
| 13     | 3    | `pad0`          | zero on the wire |
| 16     | 64   | `observed_addr` | NUL-terminated host literal; empty on PING |
| 80     | 2    | `observed_port` | zero on PING |
| 82     | 6    | `pad1`          | zero on the wire |

Padding bytes are zero on send. Receivers ignore non-zero padding
to keep forward compatibility with future field additions inside
the reserved range.

A PONG echoes the originating PING's `timestamp_us` and `seq`
verbatim, sets `flags = 0x01`, and fills `observed_addr` /
`observed_port` with the requester's apparent endpoint as the
responder sees it (sourced via `host_api->get_endpoint`).

`msg_id == 0x10` is reserved in the system message-type window;
production deployments do not remap it.

---

## 3. Extension vtable

`sdk/extensions/heartbeat.h` declares:

```c
#define GN_EXT_HEARTBEAT          "gn.heartbeat"
#define GN_EXT_HEARTBEAT_VERSION  0x00010000u

typedef struct gn_heartbeat_api_s {
    uint32_t api_size;          /* sizeof(gn_heartbeat_api_t) at producer build time */
    int (*get_stats)(void* ctx, gn_heartbeat_stats_t* out);
    int (*get_rtt)(void* ctx, gn_conn_id_t conn, uint64_t* out_rtt_us);
    int (*get_observed_address)(void* ctx, gn_conn_id_t conn,
                                 char* buf, size_t buf_len,
                                 uint16_t* port_out);
    void* ctx;
    void* _reserved[4];
} gn_heartbeat_api_t;
```

Begins with `api_size` for size-prefix evolution per
`docs/contracts/abi-evolution.md` §3. The kernel stores extension vtables as
opaque `const void*`, so the consumer (a plugin querying via
`host_api->query_extension_checked`) runs the size guard before
invoking any slot added after `MINOR` 0 — the `GN_API_HAS(api,
field)` macro from `sdk/abi.h` is the canonical check. `ctx` is
the handler's `self` pointer; every entry takes it as the first
argument.

### 3.1 `get_stats`

Snapshots aggregate RTT across every peer that has produced at
least one PONG:

```c
typedef struct gn_heartbeat_stats_s {
    uint32_t peer_count;
    uint32_t avg_rtt_us;
    uint32_t min_rtt_us;
    uint32_t max_rtt_us;
} gn_heartbeat_stats_t;
```

Returns 0 on success, `-1` when `out == NULL`. With zero peers the
RTT fields are zero and `peer_count == 0`; callers branch on
`peer_count` rather than on individual zero values.

### 3.2 `get_rtt`

Latest single-PONG RTT recorded for `conn`, in microseconds.
Returns 0 on success, `-1` when the connection is unknown or no
PONG has yet been observed. The value is the most recent
observation; the handler does not smooth or filter it.

### 3.3 `get_observed_address`

Latest external-address observation reported by `conn`. `buf` is
filled with the NUL-terminated host literal (IP or hostname) up to
`buf_len` bytes; `port_out` receives the matching port. Returns 0
on success, `-1` when the connection is unknown, no PONG has been
observed, or `buf_len` is too small to hold the address (in that
last case `buf` is left NUL-terminated at the truncation boundary
so callers do not read uninitialised bytes).

---

## 4. Dependencies on other extensions / host_api

The handler depends on four `host_api` slots:

| Slot | Use |
|---|---|
| `subscribe_conn_state` / `unsubscribe` | watch DISCONNECTED events to drop `peers_[conn]` and outstanding pings — see `docs/contracts/handler-registration.md` §3a |
| `get_endpoint` | look up the URI of `conn` to fill `observed_addr` / `observed_port` on PONG |
| `send` | emit PING / PONG payloads through the active protocol layer |

The handler reads the inbound-edge connection straight from
`gn_message_t::conn_id` per `docs/contracts/handler-registration.md` §3a;
`find_conn_by_pk` would be wrong on relay paths where `sender_pk`
identifies the originating peer but the receiving connection
belongs to the relay.

It does **not** depend on any other extension. The plugin
descriptor declares no `ext_requires`.

---

## 5. State

The handler keeps per-peer state behind a `shared_mutex`:

| Field | Meaning |
|---|---|
| `seq` | monotonic counter, atomic |
| `last_rtt_us` | latest PONG RTT, atomic |
| `missed` | counter incremented on PING-without-PONG; reset on PONG (used by external policy) |
| `observed_addr` / `observed_port` | latest peer-reported external endpoint |

`reset_state()` clears every entry; called from `on_shutdown` and
explicitly by tests. Active session state across multiple peers
is independent — one peer's missed PONG never clears another
peer's RTT.

---

## 6. Cross-references

- Wire envelope shape: `docs/contracts/protocol-layer.md` §3.
- Extension query semantics: `docs/contracts/host-api.md` §2 (`query_extension_checked`).
- Clock injection rules: `docs/contracts/clock.md` §2.
