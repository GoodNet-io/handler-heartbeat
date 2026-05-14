// SPDX-License-Identifier: Apache-2.0
/// @file   plugins/handlers/heartbeat/plugin_entry.cpp
/// @brief  Plugin entry collapsed to the `GN_HANDLER_PLUGIN` macro
///         from `sdk/cpp/handler_plugin.hpp` (2026-05-12 DX pass).
///
/// The macro generates the five `gn_plugin_*` C entry points + the
/// optional `gn_plugin_descriptor` symbol, builds the handler vtable
/// from `HeartbeatHandler`'s static metadata (`protocol_id`,
/// `msg_id`, `priority`), and registers the `gn.heartbeat` extension
/// via the class's `extension_name` / `extension_version` /
/// `extension_vtable` triplet. Prior hand-rolled implementation
/// (119 LOC, git history at 2026-05-12 pre-DX) is replaced.

#include <sdk/cpp/handler_plugin.hpp>

#include "heartbeat.hpp"

GN_HANDLER_PLUGIN(
    ::gn::handler::heartbeat::HeartbeatHandler,
    "goodnet_handler_heartbeat",
    "0.1.0")
