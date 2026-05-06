// SPDX-License-Identifier: Apache-2.0
/// @file   plugins/handlers/heartbeat/tests/test_heartbeat.cpp
/// @brief  HeartbeatHandler — PING/PONG state machine + observed-address
///         reflection + extension surface, with deterministic clock.

#include <gtest/gtest.h>

#include <plugins/handlers/heartbeat/heartbeat.hpp>

#include <sdk/extensions/heartbeat.h>
#include <sdk/host_api.h>
#include <sdk/types.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace gn::handler::heartbeat;

/// Captures `host_api->send` calls and provides scripted responses
/// for `find_conn_by_pk` / `get_endpoint`.
struct StubHost {
    std::atomic<int>                     send_calls{0};
    std::vector<std::vector<std::uint8_t>> sent_payloads;
    std::vector<gn_conn_id_t>            sent_conns;
    std::vector<std::uint32_t>           sent_msg_ids;
    std::mutex                           mu;

    /// pk[0] byte → (conn_id, uri). Tests prime via `add_peer`.
    struct PeerEntry {
        gn_conn_id_t conn;
        std::string  uri;
    };
    std::unordered_map<std::uint8_t, PeerEntry> peer_map;

    void add_peer(std::uint8_t marker, gn_conn_id_t conn, std::string uri) {
        peer_map[marker] = {conn, std::move(uri)};
    }

    static gn_result_t on_send(void* host_ctx, gn_conn_id_t conn,
                                std::uint32_t msg_id,
                                const std::uint8_t* payload, std::size_t size) {
        auto* h = static_cast<StubHost*>(host_ctx);
        std::lock_guard lk(h->mu);
        h->sent_payloads.emplace_back(payload, payload + size);
        h->sent_conns.push_back(conn);
        h->sent_msg_ids.push_back(msg_id);
        h->send_calls.fetch_add(1);
        return GN_OK;
    }

    static gn_result_t on_find_conn(void* host_ctx,
                                     const std::uint8_t pk[GN_PUBLIC_KEY_BYTES],
                                     gn_conn_id_t* out_conn) {
        auto* h = static_cast<StubHost*>(host_ctx);
        std::lock_guard lk(h->mu);
        auto it = h->peer_map.find(pk[0]);
        if (it == h->peer_map.end()) return GN_ERR_NOT_FOUND;
        *out_conn = it->second.conn;
        return GN_OK;
    }

    static gn_result_t on_get_endpoint(void* host_ctx, gn_conn_id_t conn,
                                        gn_endpoint_t* out) {
        auto* h = static_cast<StubHost*>(host_ctx);
        std::lock_guard lk(h->mu);
        for (auto& [m, p] : h->peer_map) {
            if (p.conn == conn) {
                std::memset(out, 0, sizeof(*out));
                out->conn_id = conn;
                const std::size_t n = std::min(p.uri.size(),
                                                static_cast<std::size_t>(GN_ENDPOINT_URI_MAX - 1));
                std::memcpy(out->uri, p.uri.data(), n);
                out->uri[n] = '\0';
                return GN_OK;
            }
        }
        return GN_ERR_NOT_FOUND;
    }
};

host_api_t make_stub_api(StubHost& h) {
    host_api_t api{};
    api.api_size         = sizeof(host_api_t);
    api.host_ctx         = &h;
    api.send             = &StubHost::on_send;
    api.find_conn_by_pk  = &StubHost::on_find_conn;
    api.get_endpoint     = &StubHost::on_get_endpoint;
    return api;
}

/// Mock clock with explicit `set` / `advance`. The handler accepts
/// any callable returning `uint64_t microseconds`.
struct MockClock {
    std::atomic<std::uint64_t> now{0};
    ClockNowUs as_callable() {
        return [this] { return now.load(std::memory_order_acquire); };
    }
    void set(std::uint64_t v) { now.store(v, std::memory_order_release); }
    void advance(std::uint64_t d) { now.fetch_add(d, std::memory_order_acq_rel); }
};

/// Owning envelope: the on-wire byte buffer plus a `gn_message_t`
/// pointing into it. Tests build this once per case so the wire
/// bytes outlive `handle_message()`. The struct is non-movable
/// because `msg.payload` aliases `wire.data()`; copying would leave
/// a dangling pointer. `msg` is the conventional name so call sites
/// read `hh.handle_message(&env->msg)` without the `&env->msg`
/// stutter.
struct WireEnvelope {
    std::array<std::uint8_t, kPayloadSize> wire{};
    gn_message_t msg{};

    WireEnvelope() = default;
    WireEnvelope(const WireEnvelope&)            = delete;
    WireEnvelope& operator=(const WireEnvelope&) = delete;
};

/// Build a wire envelope describing a PING/PONG sourced from a peer
/// whose `sender_pk` first byte equals @p marker. The optional
/// `conn` argument matches the conn id the kernel would have
/// stamped onto `msg.conn_id` before dispatch — passing
/// `GN_INVALID_ID` simulates a malformed dispatch path that the
/// handler must reject.
[[nodiscard]] std::unique_ptr<WireEnvelope>
make_envelope(std::uint8_t marker, const HeartbeatPayload& hb,
              gn_conn_id_t conn) {
    auto w = std::make_unique<WireEnvelope>();
    w->wire = serialize_payload(hb);
    w->msg.msg_id       = kHeartbeatMsgId;
    w->msg.sender_pk[0] = marker;
    w->msg.payload      = w->wire.data();
    w->msg.payload_size = w->wire.size();
    w->msg.conn_id      = conn;
    return w;
}

}  // namespace

// ── PING reflection ──────────────────────────────────────────────────────

TEST(Heartbeat, PingProducesPongWithObservedAddress) {
    StubHost host;
    host.add_peer(0xAA, /*conn*/ 7, "tcp://203.0.113.5:9000");
    auto api = make_stub_api(host);
    MockClock clock;
    clock.set(1'000'000);

    HeartbeatHandler hh(&api, clock.as_callable());

    HeartbeatPayload ping{};
    ping.timestamp_us = 500'000;
    ping.seq          = 42;
    ping.flags        = kFlagPing;

    auto env = make_envelope(0xAA, ping, /*conn*/ 7);
    EXPECT_EQ(hh.handle_message(&env->msg), GN_PROPAGATION_CONSUMED);

    ASSERT_EQ(host.send_calls.load(), 1);
    ASSERT_EQ(host.sent_conns.front(), 7u);
    ASSERT_EQ(host.sent_msg_ids.front(), kHeartbeatMsgId);

    ASSERT_EQ(host.sent_payloads.front().size(), kPayloadSize);
    const auto reply_opt = parse_payload(host.sent_payloads.front());
    ASSERT_TRUE(reply_opt.has_value());
    if (reply_opt) {
        const auto& reply = *reply_opt;
        EXPECT_EQ(reply.flags, kFlagPong);
        EXPECT_EQ(reply.timestamp_us, 500'000u);  /// echoed
        EXPECT_EQ(reply.seq, 42u);
        EXPECT_STREQ(reply.observed_addr, "203.0.113.5");
        EXPECT_EQ(reply.observed_port, 9000);
    }
}

TEST(Heartbeat, PongRecordsRttAndObservation) {
    StubHost host;
    host.add_peer(0xBB, /*conn*/ 11, "tcp://198.51.100.7:5000");
    auto api = make_stub_api(host);
    MockClock clock;

    HeartbeatHandler hh(&api, clock.as_callable());

    /// Send a PING at t=1.5s so the matching PONG can be matched by
    /// `seq` to the locally-stored `sent_at_us`. Per the Wave 7.1
    /// contract: RTT comes from the local timestamp, NOT the
    /// peer-echoed `timestamp_us` — a hostile peer that altered the
    /// echo would otherwise pollute the recorded RTT.
    clock.set(1'500'000);
    ASSERT_EQ(hh.send_ping(/*conn*/ 11), GN_OK);
    /// `send_ping` allocates seq starting at 0; capture the seq the
    /// stub recorded so the PONG below echoes the right value.
    ASSERT_EQ(host.send_calls.load(), 1);
    /// First seq is 0 (atomic fetch_add of `peer->seq` from default 0).
    const std::uint32_t expected_seq = 0;

    /// Advance the clock to t=2s; the PONG arrives with the same
    /// seq and the handler records `now - sent_at_us = 500ms`.
    clock.set(2'000'000);

    HeartbeatPayload pong{};
    pong.seq          = expected_seq;
    pong.timestamp_us = 999'999;       /// hostile / random echo — ignored
    pong.flags        = kFlagPong;
    std::strncpy(pong.observed_addr, "203.0.113.5",
                 sizeof(pong.observed_addr) - 1);
    pong.observed_port = 9000;

    auto env = make_envelope(0xBB, pong, /*conn*/ 11);
    EXPECT_EQ(hh.handle_message(&env->msg), GN_PROPAGATION_CONSUMED);

    /// PONG path does not call send.
    EXPECT_EQ(host.send_calls.load(), 1);

    std::uint64_t rtt = 0;
    ASSERT_EQ(hh.get_rtt(11, &rtt), 0);
    EXPECT_EQ(rtt, 500'000u);  /// 2'000'000 - 1'500'000

    char buf[64] = {};
    std::uint16_t port = 0;
    ASSERT_EQ(hh.get_observed_address(11, buf, sizeof(buf), &port), 0);
    EXPECT_STREQ(buf, "203.0.113.5");
    EXPECT_EQ(port, 9000);
}

TEST(Heartbeat, RttIsDeterministicUnderInjectedClock) {
    StubHost host;
    host.add_peer(0xCC, /*conn*/ 3, "tcp://192.0.2.1:1");
    auto api = make_stub_api(host);
    MockClock clock;

    HeartbeatHandler hh(&api, clock.as_callable());

    /// Three round-trips with different intervals — each PING is
    /// stamped at t=0 (local), the matching PONG arrives at
    /// t=interval, and the recorded RTT equals the interval. The
    /// PONG's `timestamp_us` is left at zero (peer-echoed value
    /// is ignored — Wave 7.1 contract).
    std::uint32_t seq = 0;
    for (std::uint64_t interval : {std::uint64_t{1'000},
                                     std::uint64_t{100'000},
                                     std::uint64_t{7'500}}) {
        clock.set(0);
        ASSERT_EQ(hh.send_ping(/*conn*/ 3), GN_OK);
        clock.set(interval);

        HeartbeatPayload pong{};
        pong.flags = kFlagPong;
        pong.seq   = seq++;
        auto env = make_envelope(0xCC, pong, /*conn*/ 3);
        EXPECT_EQ(hh.handle_message(&env->msg), GN_PROPAGATION_CONSUMED);

        std::uint64_t rtt = 0;
        ASSERT_EQ(hh.get_rtt(3, &rtt), 0);
        EXPECT_EQ(rtt, interval);
    }
}

TEST(Heartbeat, MultiplePeersTrackedIndependently) {
    StubHost host;
    host.add_peer(0xAA, /*conn*/ 1, "tcp://10.0.0.1:1000");
    host.add_peer(0xBB, /*conn*/ 2, "tcp://10.0.0.2:2000");
    auto api = make_stub_api(host);
    MockClock clock;

    HeartbeatHandler hh(&api, clock.as_callable());

    /// Per-peer PING, per-peer PONG. RTT is per-peer because the
    /// `outstanding_pings` map keys on `seq` per `PeerState`, and
    /// every connection has its own `PeerState`.
    {
        clock.set(50);
        ASSERT_EQ(hh.send_ping(/*conn*/ 1), GN_OK);
        clock.set(100);
        HeartbeatPayload p{};
        p.flags = kFlagPong;
        p.seq   = 0;
        auto env = make_envelope(0xAA, p, /*conn*/ 1);
        ASSERT_EQ(hh.handle_message(&env->msg), GN_PROPAGATION_CONSUMED);
    }
    {
        clock.set(100);
        ASSERT_EQ(hh.send_ping(/*conn*/ 2), GN_OK);
        clock.set(1000);
        HeartbeatPayload p{};
        p.flags = kFlagPong;
        p.seq   = 0;
        auto env = make_envelope(0xBB, p, /*conn*/ 2);
        ASSERT_EQ(hh.handle_message(&env->msg), GN_PROPAGATION_CONSUMED);
    }

    std::uint64_t r1 = 0, r2 = 0;
    EXPECT_EQ(hh.get_rtt(1, &r1), 0);
    EXPECT_EQ(hh.get_rtt(2, &r2), 0);
    EXPECT_EQ(r1, 50u);
    EXPECT_EQ(r2, 900u);
    EXPECT_EQ(hh.peer_count(), 2u);
}

TEST(Heartbeat, MalformedPayloadLeavesNoState) {
    StubHost host;
    host.add_peer(0xAA, /*conn*/ 9, "tcp://10.0.0.9:9999");
    auto api = make_stub_api(host);
    MockClock clock;

    HeartbeatHandler hh(&api, clock.as_callable());

    /// Truncated payload — handler must not consume nor allocate state.
    gn_message_t env{};
    env.msg_id = kHeartbeatMsgId;
    env.sender_pk[0] = 0xAA;
    const std::uint8_t junk[7] = {1, 2, 3, 4, 5, 6, 7};
    env.payload = junk;
    env.payload_size = sizeof(junk);

    EXPECT_EQ(hh.handle_message(&env), GN_PROPAGATION_CONTINUE);
    EXPECT_EQ(host.send_calls.load(), 0);
    EXPECT_EQ(hh.peer_count(), 0u);
}

TEST(Heartbeat, UnknownSenderRejected) {
    StubHost host;
    /// No peer entries — find_conn_by_pk returns NOT_FOUND.
    auto api = make_stub_api(host);
    MockClock clock;

    HeartbeatHandler hh(&api, clock.as_callable());

    HeartbeatPayload ping{};
    ping.flags = kFlagPing;
    auto env = make_envelope(0xFF, ping, /*conn*/ GN_INVALID_ID);
    EXPECT_EQ(hh.handle_message(&env->msg), GN_PROPAGATION_CONTINUE);
    EXPECT_EQ(host.send_calls.load(), 0);
}

// ── send_ping ────────────────────────────────────────────────────────────

TEST(Heartbeat, SendPingPopulatesPayloadFromClock) {
    StubHost host;
    host.add_peer(0xAA, /*conn*/ 5, "tcp://10.0.0.5:5555");
    auto api = make_stub_api(host);
    MockClock clock;
    clock.set(123'456);

    HeartbeatHandler hh(&api, clock.as_callable());
    EXPECT_EQ(hh.send_ping(/*conn*/ 5), GN_OK);

    ASSERT_EQ(host.send_calls.load(), 1);
    ASSERT_EQ(host.sent_payloads.front().size(), kPayloadSize);

    const auto ping_opt = parse_payload(host.sent_payloads.front());
    ASSERT_TRUE(ping_opt.has_value());
    if (ping_opt) {
        const auto& ping = *ping_opt;
        EXPECT_EQ(ping.flags, kFlagPing);
        EXPECT_EQ(ping.timestamp_us, 123'456u);
        EXPECT_EQ(ping.seq, 0u);  /// first ping uses seq 0
        EXPECT_EQ(ping.observed_addr[0], '\0');
        EXPECT_EQ(ping.observed_port, 0);
    }
}

TEST(Heartbeat, SendPingIncrementsSequence) {
    StubHost host;
    host.add_peer(0xAA, /*conn*/ 5, "tcp://10.0.0.5:5555");
    auto api = make_stub_api(host);
    MockClock clock;

    HeartbeatHandler hh(&api, clock.as_callable());
    (void)hh.send_ping(5);
    (void)hh.send_ping(5);
    (void)hh.send_ping(5);

    ASSERT_EQ(host.send_calls.load(), 3);
    const auto p0 = parse_payload(host.sent_payloads[0]);
    const auto p1 = parse_payload(host.sent_payloads[1]);
    const auto p2 = parse_payload(host.sent_payloads[2]);
    ASSERT_TRUE(p0.has_value());
    ASSERT_TRUE(p1.has_value());
    ASSERT_TRUE(p2.has_value());
    if (p0 && p1 && p2) {
        EXPECT_EQ(p0->seq, 0u);
        EXPECT_EQ(p1->seq, 1u);
        EXPECT_EQ(p2->seq, 2u);
    }
}

// ── Extension API ────────────────────────────────────────────────────────

TEST(Heartbeat, ExtensionVtablePopulatedAndFunctional) {
    StubHost host;
    host.add_peer(0xAA, /*conn*/ 1, "tcp://1.2.3.4:1000");
    auto api = make_stub_api(host);
    MockClock clock;

    HeartbeatHandler hh(&api, clock.as_callable());

    /// PING at t=1'500, PONG at t=2'000 → recorded RTT = 500us.
    /// Wave 7.1 contract: RTT comes from local memory, so a
    /// hostile peer's `timestamp_us` is ignored — set the
    /// PING manually via `send_ping` so `outstanding_pings` has
    /// a matching seq.
    clock.set(1'500);
    ASSERT_EQ(hh.send_ping(/*conn*/ 1), GN_OK);
    clock.set(2'000);
    HeartbeatPayload pong{};
    pong.flags = kFlagPong;
    pong.seq   = 0;
    auto env = make_envelope(0xAA, pong, /*conn*/ 1);
    ASSERT_EQ(hh.handle_message(&env->msg), GN_PROPAGATION_CONSUMED);

    const auto& ext = hh.extension_vtable();
    ASSERT_NE(ext.get_rtt, nullptr);
    ASSERT_NE(ext.get_stats, nullptr);
    ASSERT_NE(ext.get_observed_address, nullptr);
    EXPECT_EQ(ext.ctx, &hh);

    std::uint64_t rtt = 0;
    EXPECT_EQ(ext.get_rtt(ext.ctx, /*conn*/ 1, &rtt), 0);
    EXPECT_EQ(rtt, 500u);

    gn_heartbeat_stats_t stats{};
    EXPECT_EQ(ext.get_stats(ext.ctx, &stats), 0);
    EXPECT_EQ(stats.peer_count, 1u);
    EXPECT_EQ(stats.avg_rtt_us, 500u);
    EXPECT_EQ(stats.min_rtt_us, 500u);
    EXPECT_EQ(stats.max_rtt_us, 500u);
}

TEST(Heartbeat, ExtensionGetRttUnknownConnReturnsError) {
    StubHost host;
    auto api = make_stub_api(host);
    HeartbeatHandler hh(&api);
    std::uint64_t rtt = 0;
    EXPECT_EQ(hh.get_rtt(/*conn*/ 99, &rtt), -1);
}

TEST(Heartbeat, ExtensionGetObservedAddressTruncationReturnsError) {
    StubHost host;
    host.add_peer(0xAA, /*conn*/ 1, "tcp://10.0.0.5:5555");
    auto api = make_stub_api(host);
    MockClock clock;
    clock.set(1000);

    HeartbeatHandler hh(&api, clock.as_callable());
    HeartbeatPayload pong{};
    pong.flags = kFlagPong;
    pong.timestamp_us = 0;
    std::strncpy(pong.observed_addr, "192.0.2.42",
                 sizeof(pong.observed_addr) - 1);
    pong.observed_port = 4242;
    auto env = make_envelope(0xAA, pong, /*conn*/ 1);
    ASSERT_EQ(hh.handle_message(&env->msg), GN_PROPAGATION_CONSUMED);

    char buf[3] = {};                 /// too small for "192.0.2.42"
    std::uint16_t port = 0;
    EXPECT_EQ(hh.get_observed_address(1, buf, sizeof(buf), &port), -1);
    /// Even on truncation the buffer is NUL-terminated.
    EXPECT_EQ(buf[2], '\0');
}
