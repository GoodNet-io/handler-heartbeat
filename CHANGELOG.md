# Changelog — goodnet-handler-heartbeat

All notable changes to this plugin are listed here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions track the kernel ABI through `gn_handler_vtable_t` /
`gn.heartbeat` extension.

## [Unreleased]

### RTT samples published to the kernel

Every matched PONG now forwards its RTT measurement to the
kernel via `host_api->notify_rtt_sample`. Strategies see the
observation through `on_path_event(GN_PATH_EVENT_RTT_UPDATE)`,
so the strategy chain can rank connections by latency without
each strategy maintaining its own probe machinery. The
in-plugin `gn.heartbeat.get_rtt` extension still returns the
per-connection sample; the kernel-side republish is additive.

A new test asserts the PONG path publishes the sample through
the host API rather than just stashing it in the extension's
peer-state map.

### sdk/cpp endian helpers + GN_HANDLER_PLUGIN macro

Inline byte-swap helpers are replaced with the shared
`sdk/cpp/endian.hpp` big-endian helpers. The plugin entry
collapses to the `GN_HANDLER_PLUGIN` macro from the SDK, which
expands to the same vtable / version / name boilerplate that
the rest of the handler plugins use.

## [1.0.0-rc1] — 2026-05-12

Initial release. Two-way liveness check between connected peers
on reserved `msg_id 0x10`.

### Added

- PING / PONG envelope pair. PING is emitted on demand through
  the extension API or on a configurable schedule; PONG echoes
  the requester's timestamp plus the responder's view of the
  requester's external endpoint.
- `gn.heartbeat` extension API — `ping(conn_id)`,
  `get_rtt(conn_id)`, `get_observed_address(conn_id)`.
- Per-connection peer-state map migrated to `gn::sdk::PerConnMap`
  from the SDK so the per-conn lifecycle (DISCONNECTED prune,
  iteration under shutdown) matches the other handler plugins
  without a local re-implementation.
- Wire format + extension API documented in
  `docs/extension-api.en.md`. Reserved msg_id `0x10` declared in
  `core/kernel/system_handler_ids.hpp` (kernel-side).
