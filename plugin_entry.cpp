// SPDX-License-Identifier: Apache-2.0
/// @file   plugins/handlers/heartbeat/plugin_entry.cpp
/// @brief  `gn_plugin_*` entry symbols + handler/extension registration
///         around `HeartbeatHandler`.

#include "heartbeat.hpp"

#include <sdk/abi.h>
#include <sdk/host_api.h>
#include <sdk/plugin.h>

#include <cstdint>
#include <new>

namespace {

using gn::handler::heartbeat::HeartbeatHandler;
using gn::handler::heartbeat::kHeartbeatMsgId;
using gn::handler::heartbeat::kProtocolId;

struct HeartbeatPlugin {
    const host_api_t*                  api                 = nullptr;
    void*                              host_ctx            = nullptr;
    std::unique_ptr<HeartbeatHandler>  handler;
    gn_handler_id_t                    handler_id          = GN_INVALID_ID;
    bool                               extension_registered = false;
};

const char* const kProvidesList[] = {
    GN_EXT_HEARTBEAT,
    nullptr,
};

const gn_plugin_descriptor_t kDescriptor = {
    /* name              */ "goodnet_handler_heartbeat",
    /* version           */ "0.1.0",
    /* hot_reload_safe   */ 0,
    /* ext_requires      */ nullptr,
    /* ext_provides      */ kProvidesList,
    /* kind              */ GN_PLUGIN_KIND_HANDLER,
    /* _reserved         */ {nullptr, nullptr, nullptr, nullptr},
};

}  // namespace

extern "C" {

GN_PLUGIN_EXPORT void gn_plugin_sdk_version(std::uint32_t* major,
                                             std::uint32_t* minor,
                                             std::uint32_t* patch) {
    if (major) *major = GN_SDK_VERSION_MAJOR;
    if (minor) *minor = GN_SDK_VERSION_MINOR;
    if (patch) *patch = GN_SDK_VERSION_PATCH;
}

GN_PLUGIN_EXPORT gn_result_t gn_plugin_init(const host_api_t* api,
                                             void** out_self) {
    if (!api || !out_self) return GN_ERR_NULL_ARG;
    auto* p = new (std::nothrow) HeartbeatPlugin{};
    if (!p) return GN_ERR_OUT_OF_MEMORY;
    p->api      = api;
    p->host_ctx = api->host_ctx;
    p->handler  = std::make_unique<HeartbeatHandler>(api);
    *out_self = p;
    return GN_OK;
}

GN_PLUGIN_EXPORT gn_result_t gn_plugin_register(void* self) {
    if (!self) return GN_ERR_NULL_ARG;
    auto* p = static_cast<HeartbeatPlugin*>(self);
    if (!p->api || !p->api->register_vtable) return GN_ERR_NOT_IMPLEMENTED;

    const std::uint8_t kPriority = 240;  /// system handler — high priority
    gn_register_meta_t meta{};
    meta.api_size = sizeof(gn_register_meta_t);
    meta.name     = kProtocolId;
    meta.msg_id   = kHeartbeatMsgId;
    meta.priority = kPriority;
    const gn_result_t rc = p->api->register_vtable(
        p->host_ctx, GN_REGISTER_HANDLER, &meta,
        &p->handler->vtable(), p->handler.get(), &p->handler_id);
    if (rc != GN_OK) return rc;

    if (p->api->register_extension) {
        if (p->api->register_extension(
                p->host_ctx, GN_EXT_HEARTBEAT, GN_EXT_HEARTBEAT_VERSION,
                &p->handler->extension_vtable()) == GN_OK) {
            p->extension_registered = true;
        }
    }
    return GN_OK;
}

GN_PLUGIN_EXPORT gn_result_t gn_plugin_unregister(void* self) {
    if (!self) return GN_ERR_NULL_ARG;
    auto* p = static_cast<HeartbeatPlugin*>(self);
    if (p->extension_registered &&
        p->api && p->api->unregister_extension) {
        (void)p->api->unregister_extension(p->host_ctx, GN_EXT_HEARTBEAT);
        p->extension_registered = false;
    }
    if (p->api && p->api->unregister_vtable &&
        p->handler_id != GN_INVALID_ID) {
        (void)p->api->unregister_vtable(p->host_ctx, p->handler_id);
        p->handler_id = GN_INVALID_ID;
    }
    return GN_OK;
}

GN_PLUGIN_EXPORT void gn_plugin_shutdown(void* self) {
    delete static_cast<HeartbeatPlugin*>(self);
}

GN_PLUGIN_EXPORT const gn_plugin_descriptor_t* gn_plugin_descriptor(void) {
    return &kDescriptor;
}

}  // extern "C"
