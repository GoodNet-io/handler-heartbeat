// SPDX-License-Identifier: Apache-2.0
/// @file   plugins/handlers/heartbeat/heartbeat.hpp
/// @brief  PING/PONG handler with RTT tracking and STUN-on-the-wire
///         observed-address reflection. Exports the `gn.heartbeat`
///         extension per `sdk/extensions/heartbeat.h`.
///
/// Wire format mirrors the legacy 88-byte packed layout so any
/// existing observer that knows the structure can decode the same
/// bytes; field offsets are pinned by `static_assert`.

#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <unordered_map>

#include <sdk/cpp/wire.hpp>
#include <sdk/extensions/heartbeat.h>
#include <sdk/handler.h>
#include <sdk/host_api.h>
#include <sdk/types.h>

namespace gn::handler::heartbeat {

/// On-wire identifier for the heartbeat envelope. Reserved in the
/// system message-type window; production deployments should not
/// remap it.
inline constexpr std::uint32_t kHeartbeatMsgId = 0x10;

/// Stable protocol-id this handler binds to.
inline constexpr const char* kProtocolId = "gnet-v1";

inline constexpr std::uint8_t kFlagPing = 0x00;
inline constexpr std::uint8_t kFlagPong = 0x01;

/// Heartbeat wire frame is 88 bytes, big-endian for every multi-byte
/// integer. Layout:
///
/// | offset | size | field          |
/// |--------|------|----------------|
/// | 0      | 8    | timestamp_us   |
/// | 8      | 4    | seq            |
/// | 12     | 1    | flags          |
/// | 13     | 3    | pad0           |
/// | 16     | 64   | observed_addr  |
/// | 80     | 2    | observed_port  |
/// | 82     | 6    | pad1           |
inline constexpr std::size_t kPayloadSize       = 88;
inline constexpr std::size_t kObservedAddrBytes = 64;

/// In-memory representation of a PING/PONG payload. Not directly
/// `memcpy`'d to or from the wire — `serialize_payload` and
/// `parse_payload` translate between this struct and the canonical
/// big-endian byte layout above. The struct imposes no alignment,
/// packing, or size constraint of its own; the wire is the
/// authoritative shape.
struct HeartbeatPayload {
    std::uint64_t timestamp_us = 0;
    std::uint32_t seq          = 0;
    std::uint8_t  flags        = kFlagPing;

    /// Reflected observation: the responder fills this with what it
    /// sees as the requester's address; the requester reads it back
    /// to learn its external endpoint (STUN-on-the-wire). Empty on
    /// PING; populated on PONG.
    char          observed_addr[kObservedAddrBytes] = {};
    std::uint16_t observed_port = 0;
};

/// Encode @p hb into the canonical wire layout. Multi-byte fields go
/// big-endian; padding bytes are zeroed.
[[nodiscard]] std::array<std::uint8_t, kPayloadSize>
serialize_payload(const HeartbeatPayload& hb) noexcept;

/// Decode the wire frame at @p src. Returns `nullopt` when the input
/// length is not exactly `kPayloadSize`.
[[nodiscard]] std::optional<HeartbeatPayload>
parse_payload(std::span<const std::uint8_t> src) noexcept;

/// `gn::wire::WireSchema` binding for the heartbeat payload —
/// stateless type, never instantiated. The kernel and any
/// future cross-handler tooling can drive `serialize` / `parse`
/// through one concept, and the `static_assert` below pins this
/// payload's shape at compile time.
struct HeartbeatSchema {
    using value_type = HeartbeatPayload;
    static constexpr std::uint32_t msg_id = kHeartbeatMsgId;
    static constexpr std::size_t   size   = kPayloadSize;

    [[nodiscard]] static std::array<std::uint8_t, size>
    serialize(const value_type& v) noexcept {
        return serialize_payload(v);
    }

    [[nodiscard]] static std::optional<value_type>
    parse(std::span<const std::uint8_t> src) noexcept {
        return parse_payload(src);
    }
};

static_assert(::gn::wire::WireSchema<HeartbeatSchema>,
              "HeartbeatSchema must satisfy gn::wire::WireSchema");

/// Time source for RTT computation. Production binds to
/// `steady_clock`; tests inject a deterministic mock per
/// `clock.md` §2.
using ClockNowUs = std::function<std::uint64_t()>;

/// Default `ClockNowUs` reading microseconds from `steady_clock`.
[[nodiscard]] ClockNowUs default_clock();

/// PING/PONG handler with per-connection RTT and observed-address
/// state. Reactive only — periodic PING emission is left to the
/// caller (an application plugin or a test harness) via
/// `send_ping(conn)`. The handler implements `gn_handler_vtable_t`
/// directly through the static thunks declared at the bottom of
/// this header.
class HeartbeatHandler {
public:
    explicit HeartbeatHandler(const host_api_t* api,
                               ClockNowUs clock = default_clock());
    ~HeartbeatHandler();

    HeartbeatHandler(const HeartbeatHandler&)            = delete;
    HeartbeatHandler& operator=(const HeartbeatHandler&) = delete;

    /// Process an inbound heartbeat envelope. PING is reflected back
    /// as PONG with the requester's observed endpoint; PONG records
    /// RTT and the peer's reported observation of our own endpoint.
    [[nodiscard]] gn_propagation_t handle_message(const gn_message_t* env);

    /// Send a PING to @p conn. Used by an application plugin's
    /// periodic driver and by tests; the handler does not own a timer.
    [[nodiscard]] gn_result_t send_ping(gn_conn_id_t conn);

    /// Snapshot the aggregate RTT statistics across every peer that
    /// has produced at least one PONG.
    void snapshot_stats(gn_heartbeat_stats_t* out) const;

    /// Latest RTT for @p conn, in microseconds. Returns -1 when the
    /// connection is unknown or no PONG has been observed.
    [[nodiscard]] int get_rtt(gn_conn_id_t conn,
                              std::uint64_t* out_rtt_us) const;

    /// Latest peer-reported observation of the local node's external
    /// endpoint on @p conn. NUL-terminates @p out_buf; returns -1 on
    /// unknown / unobserved / truncation.
    [[nodiscard]] int get_observed_address(gn_conn_id_t conn,
                                            char* out_buf,
                                            std::size_t buf_size,
                                            std::uint16_t* out_port) const;

    /// Number of peers tracked. Useful for tests.
    [[nodiscard]] std::size_t peer_count() const noexcept;

    /// Clear all peer state. Idempotent; called from `on_shutdown`
    /// and explicitly by tests for isolation.
    void reset_state() noexcept;

    /// Build the C ABI vtable. The returned reference outlives the
    /// handler; `self` for every entry is `this`.
    [[nodiscard]] const gn_handler_vtable_t& vtable() const noexcept {
        return vtable_;
    }

    /// Build the extension vtable. Same lifetime as the handler.
    [[nodiscard]] const gn_heartbeat_api_t& extension_vtable() const noexcept {
        return ext_vtable_;
    }

private:
    struct PeerState {
        std::atomic<std::uint32_t> seq{0};
        std::atomic<std::uint64_t> last_rtt_us{0};
        std::atomic<std::uint32_t> missed{0};

        mutable std::mutex         mu;
        std::string                observed_addr;
        std::uint16_t              observed_port = 0;

        /// Local timestamps of every PING that has not yet been
        /// matched by a PONG. RTT is computed from this map, never
        /// from the peer-echoed `timestamp_us` — a hostile peer
        /// would otherwise pollute the recorded RTT by altering
        /// the echoed value. Cleared on PONG match and bounded
        /// implicitly by `missed` ramp-down (a reset_state /
        /// disconnect path drops the entire PeerState, taking the
        /// map with it).
        std::unordered_map<std::uint32_t, std::uint64_t> outstanding_pings;
    };

    [[nodiscard]] std::shared_ptr<PeerState> ensure_peer(gn_conn_id_t conn);
    [[nodiscard]] std::shared_ptr<PeerState> find_peer(gn_conn_id_t conn) const;

    /// Build the static vtable wired into `vtable_`.
    static const char* vtable_protocol_id(void* self);
    static void        vtable_supported_msg_ids(void* self,
                                                 const std::uint32_t** out_ids,
                                                 std::size_t* out_count);
    static gn_propagation_t vtable_handle_message(void* self,
                                                   const gn_message_t* env);

    /// Extension thunks bridging C ABI `void*` to `HeartbeatHandler*`.
    static int ext_get_stats(void* ctx, gn_heartbeat_stats_t* out);
    static int ext_get_rtt(void* ctx, gn_conn_id_t conn,
                            std::uint64_t* out_rtt_us);
    static int ext_get_observed_address(void* ctx, gn_conn_id_t conn,
                                         char* out_buf,
                                         std::size_t buf_size,
                                         std::uint16_t* out_port);

    /// Static thunk for the conn-state subscription. The kernel
    /// fires it for every CONNECTED / DISCONNECTED / TRUST_*
    /// event; the handler erases its `PeerState` on
    /// DISCONNECTED so peers do not accumulate forever.
    static void on_conn_event(void* user_data, const gn_conn_event_t* ev);

    const host_api_t*                                     api_;
    ClockNowUs                                            now_us_;
    gn_handler_vtable_t                                   vtable_{};
    gn_heartbeat_api_t                                    ext_vtable_{};

    mutable std::shared_mutex                             peers_mu_;
    std::unordered_map<gn_conn_id_t, std::shared_ptr<PeerState>> peers_;

    /// Subscription token for the conn-state channel. Kept so
    /// the dtor can unsubscribe before tearing down `peers_`.
    /// `GN_INVALID_SUBSCRIPTION_ID` until the host_api is bound
    /// or when `subscribe_conn_state` is unavailable.
    gn_subscription_id_t conn_state_sub_ = GN_INVALID_SUBSCRIPTION_ID;
};

} // namespace gn::handler::heartbeat
