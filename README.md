# goodnet-handler-heartbeat

Two-way liveness check between connected peers. Emits PING on
demand, replies with PONG echoing the requester's timestamp plus
the responder's view of the requester's external endpoint, exposes
per-connection RTT and observed-address samples through the
`gn.heartbeat` extension API.

**Kind**: handler · **Artefact**: dynamic plugin (`.so` via dlopen)
· **License**: GPL-2.0 with Linking Exception (see `LICENSE`)

## Build

In-tree, alongside the kernel:

```sh
nix build .#goodnet-handler-heartbeat
# result/lib/goodnet/plugins/libgoodnet_handler_heartbeat.so
```

Standalone, against an installed kernel SDK:

```sh
cd plugins/handlers/heartbeat
cmake -B build -DCMAKE_PREFIX_PATH=/usr/local -DBUILD_TESTING=OFF
cmake --build build
```

## Load

The kernel's `PluginManager` opens the `.so` from a manifest entry
that pins its SHA-256 digest. See `docs/install.md` and
`docs/contracts/plugin-manifest.md` in the kernel tree for the
deployment shape.

## Contract

- Wire format + extension API: [`docs/extension-api.md`](docs/extension-api.md)
- Kernel-side handler-registration contract:
  `docs/contracts/handler-registration.md`
- Reserved msg_id `0x10`: `core/kernel/system_handler_ids.hpp`
