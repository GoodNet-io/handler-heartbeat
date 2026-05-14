// SPDX-License-Identifier: Apache-2.0
#include "heartbeat.hpp"

#include <core/util/endian.hpp>

#include <sdk/convenience.h>
#include <sdk/cpp/uri.hpp>

#include <chrono>
#include <cstring>
#include <limits>

namespace gn::handler::heartbeat {

ClockNowUs default_clock() {
    return []() -> std::uint64_t {
        const auto now = std::chrono::steady_clock::now().time_since_epoch();
        return static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(now).count());
    };
}

namespace {

/// Per `kHeartbeatMsgId` registration — the handler subscribes to
/// exactly one `(protocol_id, msg_id)` pair.
constexpr std::uint32_t kSupportedMsgIds[] = {kHeartbeatMsgId};

/// Copy a NUL-padded host string into the wire payload's
/// `observed_addr` slot. Truncates rather than overflows.
void copy_observed(char dst[kObservedAddrBytes], std::string_view src) noexcept {
    std::memset(dst, 0, kObservedAddrBytes);
    const std::size_t n =
        std::min(src.size(), static_cast<std::size_t>(kObservedAddrBytes - 1));
    std::memcpy(dst, src.data(), n);
    dst[n] = '\0';
}

}  // namespace

std::array<std::uint8_t, kPayloadSize>
serialize_payload(const HeartbeatPayload& hb) noexcept {
    std::array<std::uint8_t, kPayloadSize> out{};
    ::gn::util::write_be<std::uint64_t>(
        std::span<std::uint8_t>(out.data() + 0, 8), hb.timestamp_us);
    ::gn::util::write_be<std::uint32_t>(
        std::span<std::uint8_t>(out.data() + 8, 4), hb.seq);
    out[12] = hb.flags;
    /// out[13..15] padded with the array's value-init zeros.
    std::memcpy(out.data() + 16, hb.observed_addr, kObservedAddrBytes);
    ::gn::util::write_be<std::uint16_t>(
        std::span<std::uint8_t>(out.data() + 80, 2), hb.observed_port);
    /// out[82..87] zero.
    return out;
}

std::optional<HeartbeatPayload>
parse_payload(std::span<const std::uint8_t> src) noexcept {
    if (src.size() != kPayloadSize) return std::nullopt;
    HeartbeatPayload out;
    out.timestamp_us =
        ::gn::util::read_be<std::uint64_t>(src.subspan(0, 8));
    out.seq          =
        ::gn::util::read_be<std::uint32_t>(src.subspan(8, 4));
    out.flags        = src[12];
    std::memcpy(out.observed_addr, src.data() + 16, kObservedAddrBytes);
    /// Defence-in-depth: never trust a peer-supplied string is
    /// NUL-terminated.
    out.observed_addr[kObservedAddrBytes - 1] = '\0';
    out.observed_port =
        ::gn::util::read_be<std::uint16_t>(src.subspan(80, 2));
    return out;
}

HeartbeatHandler::HeartbeatHandler(const host_api_t* api,
                                     ClockNowUs clock)
    : api_(api), now_us_(std::move(clock)), peers_(api)
{
    vtable_.api_size           = sizeof(gn_handler_vtable_t);
    vtable_.protocol_id        = &HeartbeatHandler::vtable_protocol_id;
    vtable_.supported_msg_ids  = &HeartbeatHandler::vtable_supported_msg_ids;
    vtable_.handle_message     = &HeartbeatHandler::vtable_handle_message;
    vtable_.on_result          = nullptr;  /// no per-dispatch tail work
    vtable_.on_init            = nullptr;
    vtable_.on_shutdown        = nullptr;

    ext_vtable_.api_size              = sizeof(gn_heartbeat_api_t);
    ext_vtable_.get_stats             = &HeartbeatHandler::ext_get_stats;
    ext_vtable_.get_rtt               = &HeartbeatHandler::ext_get_rtt;
    ext_vtable_.get_observed_address  = &HeartbeatHandler::ext_get_observed_address;
    ext_vtable_.ctx                   = this;

    /// PerConnMap owns its own conn-state subscription internally
    /// (see `sdk/cpp/per_conn_map.hpp`). `peers_(api)` in the
    /// initializer list both binds the subscription and wires the
    /// DISCONNECTED auto-erase semantic — no manual `subscribe`
    /// / `unsubscribe` boilerplate needed.
}

HeartbeatHandler::~HeartbeatHandler() {
    /// `peers_` (gn::sdk::PerConnMap) auto-unsubscribes in its own
    /// dtor; nothing for the handler to clean up explicitly.
    reset_state();
}

void HeartbeatHandler::reset_state() noexcept {
    peers_.clear();
}

std::size_t HeartbeatHandler::peer_count() const noexcept {
    return peers_.size();
}

gn_result_t HeartbeatHandler::send_ping(gn_conn_id_t conn) {
    if (!api_ || !api_->send) {
        gn_log_warn(api_, "heartbeat: send_ping conn=%llu host_api "
                          "missing send slot — kernel not bound?",
                    static_cast<unsigned long long>(conn));
        return GN_ERR_NOT_IMPLEMENTED;
    }
    auto peer = peers_.ensure(conn);

    HeartbeatPayload hb{};
    /// `timestamp_us` on the wire stays for compatibility with
    /// observers that decode the legacy 88-byte layout. RTT is
    /// computed from the locally-stored `sent_at_us` map, NEVER
    /// from this echoed value — see PONG path in
    /// `handle_message` for the rationale.
    const std::uint64_t sent_at_us = now_us_();
    hb.timestamp_us = sent_at_us;
    hb.seq          = peer->seq.fetch_add(1, std::memory_order_acq_rel);
    hb.flags        = kFlagPing;

    {
        std::lock_guard plk(peer->mu);
        peer->outstanding_pings[hb.seq] = sent_at_us;
    }

    const auto wire = serialize_payload(hb);
    const auto rc = api_->send(api_->host_ctx, conn, kHeartbeatMsgId,
                                wire.data(), wire.size());
    if (rc != GN_OK) {
        gn_log_warn(api_, "heartbeat: send_ping conn=%llu host_api->send "
                          "failed rc=%d (peer disconnected or "
                          "queue-bounded)",
                    static_cast<unsigned long long>(conn),
                    static_cast<int>(rc));
    }
    return rc;
}

gn_propagation_t HeartbeatHandler::handle_message(const gn_message_t* env) {
    if (!env) return GN_PROPAGATION_CONTINUE;

    auto parsed = parse_payload(
        std::span<const std::uint8_t>(env->payload, env->payload_size));
    if (!parsed) {
        /// Wrong size or other malformed input — never fall through
        /// to peer-state mutation.
        return GN_PROPAGATION_CONTINUE;
    }
    const HeartbeatPayload& hb = *parsed;

    /// Take the conn id straight from the envelope. The kernel
    /// stamped it before dispatch (`notify_inbound_bytes` thunk).
    /// `find_conn_by_pk(env->sender_pk)` is wrong on relay paths
    /// where `sender_pk` is the originating peer (set via
    /// EXPLICIT_SENDER) but the receiving connection belongs to
    /// the relay — the registry would either miss the lookup or
    /// hit a different conn entirely. `gn_message_t::conn_id` is
    /// the authoritative source.
    const gn_conn_id_t conn = env->conn_id;
    if (conn == GN_INVALID_ID) return GN_PROPAGATION_CONTINUE;

    auto peer = peers_.ensure(conn);

    if (hb.flags == kFlagPing) {
        /// Reflect the requester's endpoint back so they learn how we
        /// see their address (STUN-on-the-wire). Resolved through
        /// `get_endpoint` → URI parse.
        gn_endpoint_t ep{};
        std::string host;
        std::uint16_t port = 0;
        if (api_ && api_->get_endpoint &&
            api_->get_endpoint(api_->host_ctx, conn, &ep) == GN_OK)
        {
            const auto parts = ::gn::parse_uri(ep.uri);
            if (parts) {
                host = parts->host;
                port = parts->port;
            }
        }

        HeartbeatPayload reply = hb;
        reply.flags = kFlagPong;
        copy_observed(reply.observed_addr, host);
        reply.observed_port = port;

        if (api_ && api_->send) {
            const auto wire = serialize_payload(reply);
            (void)api_->send(api_->host_ctx, conn, kHeartbeatMsgId,
                              wire.data(), wire.size());
        }
        return GN_PROPAGATION_CONSUMED;
    }

    if (hb.flags == kFlagPong) {
        /// RTT comes from the locally stored `sent_at_us` keyed
        /// by `seq`, NOT from the peer-echoed `timestamp_us` —
        /// otherwise a hostile peer could pollute the recorded
        /// RTT by altering the echoed value (always emit a
        /// PONG with `timestamp_us = 0` and the local computer
        /// would record the full wall-clock time as the RTT).
        const std::uint64_t now = now_us_();
        std::uint64_t rtt = 0;
        bool matched = false;
        {
            std::lock_guard plk(peer->mu);
            if (auto it = peer->outstanding_pings.find(hb.seq);
                it != peer->outstanding_pings.end()) {
                rtt = (now > it->second) ? (now - it->second) : 0;
                peer->outstanding_pings.erase(it);
                matched = true;
            }
        }
        if (!matched) {
            /// PONG carrying a `seq` we never sent — drop the
            /// stat update rather than write a meaningless RTT.
            return GN_PROPAGATION_CONSUMED;
        }
        peer->last_rtt_us.store(rtt, std::memory_order_release);
        peer->missed.store(0, std::memory_order_release);

        /// Latest peer-reported view of our address; `parse_payload`
        /// already enforced the trailing NUL.
        if (hb.observed_addr[0] != '\0' && hb.observed_port != 0) {
            std::lock_guard plk(peer->mu);
            peer->observed_addr.assign(hb.observed_addr);
            peer->observed_port = hb.observed_port;
        }
        return GN_PROPAGATION_CONSUMED;
    }

    /// Unknown flag — leave for the next handler to decide.
    return GN_PROPAGATION_CONTINUE;
}

void HeartbeatHandler::snapshot_stats(gn_heartbeat_stats_t* out) const {
    if (!out) return;
    out->peer_count = 0;
    out->avg_rtt_us = 0;
    out->min_rtt_us = 0;
    out->max_rtt_us = 0;

    std::uint64_t sum = 0;
    std::uint32_t mn = std::numeric_limits<std::uint32_t>::max();
    std::uint32_t mx = 0;
    std::uint32_t count = 0;

    peers_.for_each([&](gn_conn_id_t /*conn*/,
                         const std::shared_ptr<PeerState>& peer) {
        const std::uint32_t rtt = static_cast<std::uint32_t>(
            peer->last_rtt_us.load(std::memory_order_acquire));
        if (rtt == 0) return;
        sum += rtt;
        if (rtt < mn) mn = rtt;
        if (rtt > mx) mx = rtt;
        ++count;
    });

    out->peer_count = count;
    out->avg_rtt_us = (count > 0) ? static_cast<std::uint32_t>(sum / count) : 0;
    out->min_rtt_us = (count > 0) ? mn : 0;
    out->max_rtt_us = mx;
}

int HeartbeatHandler::get_rtt(gn_conn_id_t conn,
                                std::uint64_t* out_rtt_us) const {
    if (!out_rtt_us) return -1;
    auto peer = peers_.find(conn);
    if (!peer) return -1;
    const std::uint64_t rtt = peer->last_rtt_us.load(std::memory_order_acquire);
    if (rtt == 0) return -1;
    *out_rtt_us = rtt;
    return 0;
}

int HeartbeatHandler::get_observed_address(gn_conn_id_t conn,
                                             char* out_buf,
                                             std::size_t buf_size,
                                             std::uint16_t* out_port) const {
    if (!out_buf || buf_size == 0) return -1;
    auto peer = peers_.find(conn);
    if (!peer) return -1;

    std::lock_guard plk(peer->mu);
    if (peer->observed_addr.empty()) return -1;

    const std::size_t n = peer->observed_addr.size();
    if (n + 1 > buf_size) {
        std::memcpy(out_buf, peer->observed_addr.data(), buf_size - 1);
        out_buf[buf_size - 1] = '\0';
        if (out_port) *out_port = peer->observed_port;
        return -1;
    }
    std::memcpy(out_buf, peer->observed_addr.data(), n);
    out_buf[n] = '\0';
    if (out_port) *out_port = peer->observed_port;
    return 0;
}

// ── static thunks ────────────────────────────────────────────────────────

const char* HeartbeatHandler::vtable_protocol_id(void* /*self*/) {
    return kProtocolId;
}

void HeartbeatHandler::vtable_supported_msg_ids(void* /*self*/,
                                                  const std::uint32_t** out_ids,
                                                  std::size_t* out_count) {
    if (out_ids)   *out_ids = kSupportedMsgIds;
    if (out_count) *out_count = std::size(kSupportedMsgIds);
}

gn_propagation_t HeartbeatHandler::vtable_handle_message(void* self,
                                                           const gn_message_t* env) {
    return static_cast<HeartbeatHandler*>(self)->handle_message(env);
}

int HeartbeatHandler::ext_get_stats(void* ctx, gn_heartbeat_stats_t* out) {
    if (!ctx || !out) return -1;
    static_cast<HeartbeatHandler*>(ctx)->snapshot_stats(out);
    return 0;
}

int HeartbeatHandler::ext_get_rtt(void* ctx, gn_conn_id_t conn,
                                    std::uint64_t* out_rtt_us) {
    if (!ctx) return -1;
    return static_cast<HeartbeatHandler*>(ctx)->get_rtt(conn, out_rtt_us);
}

int HeartbeatHandler::ext_get_observed_address(void* ctx, gn_conn_id_t conn,
                                                 char* out_buf,
                                                 std::size_t buf_size,
                                                 std::uint16_t* out_port) {
    if (!ctx) return -1;
    return static_cast<HeartbeatHandler*>(ctx)->get_observed_address(
        conn, out_buf, buf_size, out_port);
}

}  // namespace gn::handler::heartbeat
