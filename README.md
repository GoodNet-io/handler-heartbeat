# goodnet-handler-heartbeat

Two-way liveness check between connected peers. Emits PING on
demand, replies with PONG echoing the requester's timestamp plus
the responder's view of the requester's external endpoint, exposes
per-connection RTT and observed-address samples through the
`gn.heartbeat` extension API. Every matched PONG-driven sample
also forwards to the kernel via `host_api->notify_rtt_sample`
so strategies see the observation through their
`on_path_event(GN_PATH_EVENT_RTT_UPDATE)` channel — the
strategy chain ranks conns by latency without each strategy
maintaining its own probe.

**Kind**: handler · **Artefact**: dynamic plugin (`.so` via dlopen)
· **License**: GPL-2.0 with Linking Exception (see `LICENSE`)

## Build

This plugin lives in its own git with a flake that pulls the
kernel SDK as a Nix input. From this checkout:

```sh
nix run .#build         # release build of libgoodnet_handler_heartbeat.so
nix run .#test          # vanilla ctest
nix run .#test-asan     # AddressSanitizer + UBSan
nix run .#test-tsan     # ThreadSanitizer
```

The kernel monorepo also builds this plugin in-tree through its
own `nix run .#build -- release` — operator install consumes
every bundled `.so` from there.

## Load

The kernel's `PluginManager` opens the `.so` from a manifest entry
that pins its SHA-256 digest. See `docs/install.en.md` and
`docs/contracts/plugin-manifest.en.md` in the kernel tree for the
deployment shape.

## Contract

- Wire format + extension API: [`docs/extension-api.md`](docs/extension-api.md)
- Kernel-side handler-registration contract:
  `docs/contracts/handler-registration.en.md`
- Reserved msg_id `0x10`: `core/kernel/system_handler_ids.hpp`
