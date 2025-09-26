#ifndef TYPES_HPP
#define TYPES_HPP

#include <cstdint>
#include <atomic>
#include <chrono>

#pragma pack(push, 1) // Ensure no padding for cache alignment
struct MarketDataPoint {
    double bid_px;
    double ask_px;
    std::int64_t timestamp_delta; // Delta from a base timestamp, or raw epoch ns
    std::int32_t instrument_id;   // Internal ID for the instrument
    std::uint32_t bid_sz;
    std::uint32_t ask_sz;
    
    // Default constructor for queue initialization
    MarketDataPoint() : bid_px(0.0), ask_px(0.0), timestamp_delta(0), 
                       instrument_id(0), bid_sz(0), ask_sz(0) {}
    
    // Utility function to get current timestamp
    static std::int64_t current_timestamp_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()
        ).count();
    }
};
#pragma pack(pop)

// Performance metrics for MPMC queue monitoring
struct PerformanceMetrics {
    std::atomic<uint64_t> messages_received{0};
    std::atomic<uint64_t> messages_processed{0};
    std::atomic<uint64_t> total_latency_ns{0};
    std::atomic<uint64_t> max_latency_ns{0};
    std::atomic<uint64_t> buffer_overruns{0};     // Failed pushes (queue full)
    std::atomic<uint64_t> buffer_underruns{0};    // Failed pops (queue empty)
    
    // Calculate average latency
    double avg_latency_us() const {
        auto processed = messages_processed.load();
        if (processed == 0) return 0.0;
        return static_cast<double>(total_latency_ns.load()) / static_cast<double>(processed) / 1000.0;

    }
    
    // Push success rate
    double push_success_rate() const {
        auto received = messages_received.load();
        if (received == 0) return 0.0;
        return 1.0 - (static_cast<double>(buffer_overruns.load()) / static_cast<double>(received));

    }

    // Reset all metrics
    void Reset() {
        messages_received.store(0);
        messages_processed.store(0);
        total_latency_ns.store(0);
        max_latency_ns.store(0);
        buffer_overruns.store(0);
        buffer_underruns.store(0);
    }
};

// ================= VWAP Tracking ==================
struct VWAPTracker {
    double cum_px_qty{0.0};
    double cum_qty{0.0};

    void add(double price, double qty) {
        cum_px_qty += price * qty;
        cum_qty += qty;
    }

    double vwap() const {
        return cum_qty > 0 ? cum_px_qty / cum_qty : 0.0;
    }
};

// Holds per-instrument stats (VWAP + counters, extendable later)
struct InstrumentStats {
    VWAPTracker vwap_tracker;
    uint64_t trades_processed{0};

    void update(double price, double qty) {
        vwap_tracker.add(price, qty);
        trades_processed++;
    }
};

#endif // TYPES_HPP