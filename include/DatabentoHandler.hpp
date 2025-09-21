// DatabentoHandler.hpp
#pragma once

#include "Types.hpp"
#include "LockFreeRingBuffer.hpp"
#include <databento.hpp>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <functional>
#include <chrono>

namespace market_data {

enum class DataMode {
    HISTORICAL,
    LIVE
};

struct DatabentoConfig {
    std::string api_key;
    std::vector<std::string> symbols;
    std::string dataset = "XNAS.ITCH";  // Default to NASDAQ
    databento::Schema schema = databento::Schema::Mbp1;
    DataMode mode = DataMode::HISTORICAL;
    
    // Historical data parameters
    std::string start_date;  // Format: "YYYY-MM-DD"
    std::string end_date;    // Format: "YYYY-MM-DD"
    
    // Live data parameters
    std::string live_gateway = "databento-equities-lsg";
    
    // Rate limiting for historical replay
    uint64_t max_messages_per_second = 0;  // 0 = no limit
};

class DatabentoHandler {
private:
    DatabentoConfig config_;
    std::unique_ptr<databento::Historical> historical_client_;
    std::unique_ptr<databento::Live> live_client_;
    
    LockFreeRingBuffer<MarketDataPoint, 65536>* buffer_;
    PerformanceMetrics* metrics_;
    
    std::atomic<bool> running_{false};
    std::atomic<bool> connected_{false};
    std::thread worker_thread_;
    
    // Symbol to instrument ID mapping
    std::unordered_map<std::string, std::int32_t> symbol_to_id_;
    std::int32_t next_instrument_id_ = 1;
    
    // Rate limiting for historical data
    std::chrono::steady_clock::time_point last_message_time_;
    std::chrono::nanoseconds message_interval_{0};
    
    // Convert Databento record to our format
    MarketDataPoint convert_mbp1_record(const databento::MbpMsg& msg);
    MarketDataPoint convert_trade_record(const databento::TradeMsg& msg);
    
    // Get or create instrument ID for symbol
    std::int32_t get_instrument_id(const std::string& symbol);
    
    // Processing methods
    void process_historical_data();
    void process_live_data();
    void handle_rate_limiting();
    
    // Error handling
    void handle_databento_error(const std::exception& e);
    
public:
    DatabentoHandler(const DatabentoConfig& config,
                    LockFreeRingBuffer<MarketDataPoint, 65536>* buffer,
                    PerformanceMetrics* metrics);
    
    ~DatabentoHandler();
    
    // Start data processing
    bool start();
    
    // Stop data processing
    void stop();
    
    // Check if handler is running
    bool is_running() const { return running_.load(); }
    bool is_connected() const { return connected_.load(); }
    
    // Get symbol mapping
    const std::unordered_map<std::string, std::int32_t>& get_symbol_mapping() const {
        return symbol_to_id_;
    }
};

} // namespace market_data