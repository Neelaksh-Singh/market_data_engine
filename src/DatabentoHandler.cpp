#include "../include/Types.hpp"
#include "../include/LockFreeRingBuffer.hpp"
#include <iostream>
#include <thread>
#include <random>
#include <chrono>
#include <iomanip>

namespace market_data {

/**
 * Each handler instance can run in its own thread for MPMC scenario
 */
class DatabentoHandler {
private:
    LockFreeRingBuffer<MarketDataPoint, 65536>& buffer_;
    std::atomic<bool> running_{false};
    PerformanceMetrics& metrics_;
    
    // Base timestamp for delta calculations
    const std::int64_t base_timestamp_;
    
    // Producer ID for differentiation
    const int producer_id_;
    
    // Instrument configuration for this producer
    struct InstrumentConfig {
        std::int32_t id;
        std::string symbol;
        double base_price;
        double last_bid;
        double last_ask;
    };
    
    std::vector<InstrumentConfig> instruments_;
    
public:
    DatabentoHandler(LockFreeRingBuffer<MarketDataPoint, 65536>& buffer, 
                     PerformanceMetrics& metrics, 
                     int producer_id = 0,
                     int num_instruments = 10)
        : buffer_(buffer), metrics_(metrics), 
          base_timestamp_(MarketDataPoint::current_timestamp_ns()),
          producer_id_(producer_id) {
        
        // Initialize instruments for this producer
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<double> price_dist(50.0, 500.0);
        
        for (int i = 0; i < num_instruments; ++i) {
            InstrumentConfig config;
            config.id = producer_id * 1000 + i + 1;  // Unique IDs per producer
            config.symbol = "SYM" + std::to_string(config.id);
            config.base_price = price_dist(gen);
            config.last_bid = config.base_price - 0.01;
            config.last_ask = config.base_price + 0.01;
            instruments_.push_back(config);
        }
    }
    
    /**
     * Start the market data feed for this producer.
     * Multiple DatabentoHandler instances can run concurrently.
     */
    void start_feed() {
        running_ = true;
        
        std::cout << "Producer " << producer_id_ << " starting market data feed with " 
                  << instruments_.size() << " instruments...\n";
        
        // Random number generation for realistic market data
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> instrument_dist(0, instruments_.size() - 1);
        std::normal_distribution<> price_change_dist(0.0, 0.001);  // Small price movements
        std::exponential_distribution<> size_dist(1000.0);  // Order sizes
        
        auto start_time = std::chrono::high_resolution_clock::now();
        std::size_t messages_sent = 0;
        std::size_t failed_sends = 0;
        
        while (running_) {
            // Generate realistic message bursts (100-500 messages per batch)
            auto messages_this_batch = std::uniform_int_distribution<>(100, 500)(gen);
            
            for (int i = 0; i < messages_this_batch && running_; ++i) {
                MarketDataPoint mdp;
                
                // Select random instrument
                auto& instrument = instruments_[instrument_dist(gen)];
                mdp.instrument_id = instrument.id;
                
                // Generate price movement
                double price_change = price_change_dist(gen);
                instrument.last_bid += price_change;
                instrument.last_ask = instrument.last_bid + 0.01;  // 1 cent spread
                
                mdp.bid_px = instrument.last_bid;
                mdp.ask_px = instrument.last_ask;
                mdp.bid_sz = static_cast<std::uint32_t>(size_dist(gen));
                mdp.ask_sz = static_cast<std::uint32_t>(size_dist(gen));
                mdp.timestamp_delta = MarketDataPoint::current_timestamp_ns() - base_timestamp_;
                
                // Attempt to push to MPMC queue
                if (buffer_.try_push(mdp)) {
                    messages_sent++;
                    metrics_.messages_received.fetch_add(1);
                } else {
                    failed_sends++;
                    metrics_.buffer_overruns.fetch_add(1);
                }
            }
            
            // Control message rate - aim for ~20k messages/second per producer
            std::this_thread::sleep_for(std::chrono::microseconds(20));
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        
        std::cout << "Producer " << producer_id_ << " stopped. Sent " << messages_sent 
                  << " messages (failed: " << failed_sends << ") in " << duration << "ms\n";
    }
    
    void stop_feed() {
        running_ = false;
    }
    
    int get_producer_id() const {
        return producer_id_;
    }
    
    const std::vector<InstrumentConfig>& get_instruments() const {
        return instruments_;
    }
};

} // namespace market_data