#pragma once

#include "LockFreeRingBuffer.hpp"
#include "Types.hpp"
#include <databento/historical.hpp>
#include <databento/dbn.hpp>
#include <databento/symbol_map.hpp>
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <functional>

namespace market_data {

/**
 * DatabentoHandler - Handles historical data fetching from Databento API
 * and pushes MarketDataPoint objects to the MPMC lock-free queue.
 * 
 * Currently supports BBO (Best Bid Offer) schema for historical data.
 */
class DatabentoHandler {
public:
    // Constructor with configuration
    explicit DatabentoHandler(const std::string& api_key,
                             size_t queue_size = 1024 * 1024);  // Default 1M buffer
    
    // Destructor
    ~DatabentoHandler();
    
    // Non-copyable
    DatabentoHandler(const DatabentoHandler&) = delete;
    DatabentoHandler& operator=(const DatabentoHandler&) = delete;
    
    /**
     * Initialize the handler with API key from environment
     */
    static std::unique_ptr<DatabentoHandler> CreateFromEnv(size_t queue_size = 1024 * 1024);
    
    /**
     * Fetch historical BBO data and push to queue
     * 
     * @param dataset The dataset to fetch from (e.g., "GLBX.MDP3")
     * @param symbols List of symbols to fetch
     * @param start_time Start time in ISO 8601 format
     * @param end_time End time in ISO 8601 format
     * @param schema BBO schema variant ("bbo-1s" or "bbo-1m")
     * @param stype_in Symbol type for input (default: Parent)
     * @return true if successful, false otherwise
     */
    bool FetchHistoricalBBO(
        const std::string& dataset,
        const std::vector<std::string>& symbols,
        const std::string& start_time,
        const std::string& end_time,
        const std::string& schema = "bbo-1s",
        databento::SType stype_in = databento::SType::Parent
    );
    
    /**
     * Start asynchronous data fetching in a separate thread
     */
    void StartAsyncFetch(
        const std::string& dataset,
        const std::vector<std::string>& symbols,
        const std::string& start_time,
        const std::string& end_time,
        const std::string& schema = "bbo-1s",
        databento::SType stype_in = databento::SType::Parent
    );
    
    /**
     * Stop asynchronous fetching
     */
    void StopAsyncFetch();
    
    /**
     * Get access to the underlying queue for consumers
     */
    LockFreeRingBuffer<MarketDataPoint, 1024 * 1024>& GetQueue() { return *data_queue_; }
    
    /**
     * Get performance metrics
     */
    const PerformanceMetrics& GetMetrics() const { return metrics_; }
    
    /**
     * Check if handler is currently fetching data
     */
    bool IsFetching() const { return is_fetching_.load(); }
    
    /**
     * Set callback for error handling
     */
    void SetErrorCallback(std::function<void(const std::string&)> callback) {
        error_callback_ = callback;
    }
    
private:
    /**
     * Process BBO records and convert to MarketDataPoint
     */
    void ProcessBBORecord(
        const databento::Record& record,
        const databento::TsSymbolMap& symbol_map,
        const std::string& dataset
    );
    
    /**
     * Convert Databento fixed-price to double
     */
    double ConvertPrice(int64_t fixed_price) const;
    
    /**
     * Async fetch worker thread function
     */
    void AsyncFetchWorker(
        const std::string& dataset,
        const std::vector<std::string>& symbols,
        const std::string& start_time,
        const std::string& end_time,
        const std::string& schema,
        databento::SType stype_in
    );
    
    // Member variables
    std::unique_ptr<databento::Historical> client_;
    std::unique_ptr<LockFreeRingBuffer<MarketDataPoint, 1024 * 1024>> data_queue_;
    PerformanceMetrics metrics_;
    std::atomic<bool> is_fetching_{false};
    std::unique_ptr<std::thread> fetch_thread_;
    std::function<void(const std::string&)> error_callback_;
    
    // Constants
    static constexpr int64_t PRICE_SCALE = 1000000000LL;  // 1e9 for fixed-point conversion
    static constexpr int64_t UNDEF_PRICE = 9223372036854775807LL;  // INT64_MAX
};

} // namespace market_data