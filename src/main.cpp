#include "../include/DatabentoHandler.hpp"
#include "../include/LockFreeRingBuffer.hpp"
#include "../include/Types.hpp"
#include "../include/Config.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <signal.h>
#include <unordered_map>

using namespace market_data;

// Global flag for graceful shutdown
std::atomic<bool> running{true};

// Signal handler for graceful shutdown
void signal_handler(int sig) {
    std::cout << "\nReceived signal " << sig << ", shutting down..." << std::endl;
    running = false;
}

// Consumer function that reads from the queue
void consumer_thread(LockFreeRingBuffer<MarketDataPoint, 1024 * 1024>& queue,
                     PerformanceMetrics& metrics) {
    MarketDataPoint dp;
    size_t processed = 0;
    auto last_report = std::chrono::steady_clock::now();

    // Per-instrument stats (VWAP + counters)
    std::unordered_map<int, InstrumentStats> instrument_stats;

    while (running.load()) {
        if (queue.try_pop(dp)) {
            processed++;
            metrics.messages_processed.fetch_add(1, std::memory_order_relaxed);

            // Midpoint + approximate size as "trade"
            double qty = (dp.bid_sz + dp.ask_sz) / 2.0;
            double mid = (dp.bid_px + dp.ask_px) / 2.0;
            instrument_stats[dp.instrument_id].update(mid, qty);

            // Print sample data every 1000 messages
            if (processed % 1000 == 1) {
                std::cout << "Sample data point " << processed << ":\n";
                std::cout << "  Instrument ID: " << dp.instrument_id << "\n";
                std::cout << "  Bid: " << dp.bid_px << " @ " << dp.bid_sz << "\n";
                std::cout << "  Ask: " << dp.ask_px << " @ " << dp.ask_sz << "\n";
                std::cout << "  Timestamp: " << dp.timestamp_delta << "\n";
                std::cout << "  VWAP[" << dp.instrument_id << "]: "
                          << instrument_stats[dp.instrument_id].vwap_tracker.vwap()
                          << "\n\n";
            }
        } else {
            // No data available, small delay to prevent busy waiting
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }

        // Report metrics every 5 seconds
        auto now = std::chrono::steady_clock::now();
        if (now - last_report > std::chrono::seconds(5)) {
            std::cout << "=== Consumer Status Report ===\n";
            std::cout << "Processed: " << processed << "\n";
            std::cout << "Queue size: " << queue.size() << "\n";
            std::cout << "Queue utilization: " << queue.utilization() * 100.0 << "%\n";
            std::cout << "Messages received: " << metrics.messages_received.load() << "\n";
            std::cout << "Buffer overruns: " << metrics.buffer_overruns.load() << "\n";
            std::cout << "Avg latency: " << metrics.avg_latency_us() << " μs\n";
            std::cout << "Push success rate: " << metrics.push_success_rate() * 100.0 << "%\n";

            // Per-instrument VWAP summary
            for (auto& [id, stats] : instrument_stats) {
                std::cout << "VWAP[" << id << "]: "
                          << stats.vwap_tracker.vwap()
                          << " (trades=" << stats.trades_processed << ")\n";
            }

            std::cout << "===============================\n\n";
            last_report = now;
        }
    }

    // Final VWAP summary before exit
    std::cout << "\n=== Final VWAP Summary ===\n";
    for (auto& [id, stats] : instrument_stats) {
        std::cout << "Instrument " << id
                  << " VWAP=" << stats.vwap_tracker.vwap()
                  << " (trades=" << stats.trades_processed << ")\n";
    }
    std::cout << "===========================\n";

    std::cout << "Consumer thread exiting. Total processed: " << processed << std::endl;
}

int main() {
    // Set up signal handler
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    std::cout << "=== Databento MPMC Queue Demo ===\n";
    std::cout << "This demo fetches historical BBO data and processes it using\n";
    std::cout << "a multi-producer multi-consumer lock-free queue.\n\n";

    try {
        // Create Databento handler using environment variable for API key
        std::cout << "Creating Databento handler...\n";
        auto handler = DatabentoHandler::CreateFromEnv(config::QUEUE_SIZE);

        // Set error callback
        handler->SetErrorCallback([](const std::string& error) {
            std::cerr << "ERROR: " << error << std::endl;
        });

        // Start consumer thread
        std::cout << "Starting consumer thread...\n";
        std::thread consumer(consumer_thread,
                     std::ref(handler->GetQueue()),
                     std::ref(const_cast<PerformanceMetrics&>(handler->GetMetrics())));

        // Configuration for data fetch
        std::string dataset     = config::DATASET;
        auto symbols            = config::SYMBOLS;
        std::string start_time  = config::START_TIME;
        std::string end_time    = config::END_TIME;
        std::string schema      = config::SCHEMA;

        std::cout << "Fetching historical data...\n";
        std::cout << "Dataset: " << dataset << "\n";
        std::cout << "Symbols: ";
        for (const auto& sym : symbols) std::cout << sym << " ";
        std::cout << "\nTime range: " << start_time << " to " << end_time << "\n";
        std::cout << "Schema: " << schema << "\n\n";

        // Start async fetch
        handler->StartAsyncFetch(dataset, symbols, start_time, end_time, schema);

        // Wait for fetch to complete or user interrupt
        int wait_count = 0;
        while (handler->IsFetching() && running.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            wait_count++;
            std::cout << "Waiting... " << wait_count << " seconds" << std::endl;

            if (wait_count > config::FETCH_TIMEOUT_SECONDS) { // Default timeout after 30s
                std::cout << "Timeout waiting for data fetch\n";
                break;
            }
        }

        // If fetch completed, wait a bit more for consumer to process remaining data
        if (!handler->IsFetching()) {
            std::cout << "Fetch completed. Waiting for consumer to process remaining data...\n";
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }

        // Stop consumer
        running = false;
        if (consumer.joinable()) {
            consumer.join();
        }

        // Final metrics report
        const auto& metrics = handler->GetMetrics();
        std::cout << "\n=== Final Metrics Report ===\n";
        std::cout << "Messages received: " << metrics.messages_received.load() << "\n";
        std::cout << "Messages processed: " << metrics.messages_processed.load() << "\n";
        std::cout << "Buffer overruns: " << metrics.buffer_overruns.load() << "\n";
        std::cout << "Buffer underruns: " << metrics.buffer_underruns.load() << "\n";
        std::cout << "Average latency: " << metrics.avg_latency_us() << " μs\n";
        std::cout << "Maximum latency: " << metrics.max_latency_ns.load() << " ns\n";
        std::cout << "Push success rate: " << metrics.push_success_rate() * 100.0 << "%\n";
        std::cout << "=============================\n";

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "Thank you for using the Databento MPMC Queue Demo!\n";
    return 0;
}
