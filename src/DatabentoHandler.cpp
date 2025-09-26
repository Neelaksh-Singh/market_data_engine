#include "../include/DatabentoHandler.hpp"
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <chrono>

namespace market_data {

// Constructor
DatabentoHandler::DatabentoHandler(const std::string& api_key, size_t queue_size)
    : data_queue_(std::make_unique<LockFreeRingBuffer<MarketDataPoint, 1024 * 1024>>()) {
    (void)queue_size; 
    
    try {
        client_ = std::make_unique<databento::Historical>(
            databento::Historical::Builder()
                .SetKey(api_key)
                .Build());
    } catch (const std::exception& e) {
        std::ostringstream oss;
        oss << "Failed to create Databento client: " << e.what();
        throw std::runtime_error(oss.str());
    }
}

// Destructor
DatabentoHandler::~DatabentoHandler() {
    StopAsyncFetch();
}

// Create from environment
std::unique_ptr<DatabentoHandler> DatabentoHandler::CreateFromEnv(size_t queue_size) {
    // Get API key from environment variable
    const char* api_key_env = std::getenv("DATABENTO_API_KEY");
    if (!api_key_env) {
        throw std::runtime_error("DATABENTO_API_KEY environment variable not set");
    }
    
    std::string api_key(api_key_env);
    if (api_key.empty()) {
        throw std::runtime_error("DATABENTO_API_KEY environment variable is empty");
    }
    
    return std::make_unique<DatabentoHandler>(api_key, queue_size);
}

// Fetch historical BBO data
bool DatabentoHandler::FetchHistoricalBBO(
    const std::string& dataset,
    const std::vector<std::string>& symbols,
    const std::string& start_time,
    const std::string& end_time,
    const std::string& schema,
    databento::SType stype_in) {
    
    // if (is_fetching_.load()) {
    //     if (error_callback_) {
    //         error_callback_("Already fetching data");
    //     }
    //     return false;
    // }
    
    // is_fetching_ = true;
    
    try {
        // Reset metrics for new fetch
        metrics_.Reset();  // Explicitly reset the metrics object
        
        databento::TsSymbolMap symbol_map;
        auto decode_symbols = [&symbol_map](const databento::Metadata& metadata) {
            symbol_map = metadata.CreateSymbolMap();
        };
        
        // Process each record
        auto process_record = [this, &symbol_map, &dataset](const databento::Record& record) {
            ProcessBBORecord(record, symbol_map, dataset);
            return databento::KeepGoing::Continue;
        };
        
        // Determine schema enum
        databento::Schema schema_enum;
        if (schema == "bbo-1s") {
            schema_enum = databento::Schema::Bbo1S;
        } else if (schema == "bbo-1m") {
            schema_enum = databento::Schema::Bbo1M;
        } else {
            std::ostringstream oss;
            oss << "Unsupported schema: " << schema;
            throw std::runtime_error(oss.str());
        }
        
        // Fetch data
            client_->TimeseriesGetRange(
                dataset,
                databento::DateTimeRange<std::string>{start_time, end_time},
                symbols,
                schema_enum,
                stype_in,
                databento::SType::InstrumentId,
                0, // no limit
                decode_symbols,
                process_record
            );
        
        is_fetching_ = false;
        return true;
        
    } catch (const std::exception& e) {
        is_fetching_ = false;
        std::ostringstream oss;
        oss << "Failed to fetch historical data: " << e.what();
        if (error_callback_) {
            error_callback_(oss.str());
        }
        return false;
    }
}

// Start asynchronous fetch
void DatabentoHandler::StartAsyncFetch(
    const std::string& dataset,
    const std::vector<std::string>& symbols,
    const std::string& start_time,
    const std::string& end_time,
    const std::string& schema,
    databento::SType stype_in) {
    
    if (is_fetching_.load()) {
        if (error_callback_) {
            error_callback_("Already fetching data");
        }
        return;
    }
    
    // Stop any existing thread
    StopAsyncFetch();
    
    // Start new thread FIRST, then set the flag
    fetch_thread_ = std::make_unique<std::thread>(
        &DatabentoHandler::AsyncFetchWorker,
        this,
        dataset,
        symbols,
        start_time,
        end_time,
        schema,
        stype_in
    );
    
    // Set flag after thread starts
    is_fetching_ = true;
}

// Stop asynchronous fetch
void DatabentoHandler::StopAsyncFetch() {
    is_fetching_ = false;
    
    if (fetch_thread_ && fetch_thread_->joinable()) {
        fetch_thread_->join();
        fetch_thread_.reset();
    }
}

// Process BBO record
void DatabentoHandler::ProcessBBORecord(
    const databento::Record& record,
    const databento::TsSymbolMap& symbol_map,
    const std::string& dataset) {

    (void)symbol_map;
    (void)dataset;
    
    // Get BBO message
    if (auto* bbo_msg = record.GetIf<databento::Bbo1MMsg>()) {
        
        // Create MarketDataPoint
        MarketDataPoint data_point;
        
        // Convert timestamp (ts_event is in nanoseconds since UNIX epoch)
        data_point.timestamp_delta = bbo_msg->ts_recv.time_since_epoch().count();
        
        // Set instrument ID
        data_point.instrument_id = bbo_msg->hd.instrument_id;
        
        // Convert bid/ask prices from fixed-point to double
        data_point.bid_px = ConvertPrice(bbo_msg->levels[0].bid_px);
        data_point.ask_px = ConvertPrice(bbo_msg->levels[0].ask_px);
        
        // Set bid/ask sizes
        data_point.bid_sz = bbo_msg->levels[0].bid_sz;
        data_point.ask_sz = bbo_msg->levels[0].ask_sz;
        
        // Try to push to queue
        auto start_time = std::chrono::high_resolution_clock::now();
        
        if (data_queue_->try_push(data_point)) {
            // Success - update metrics
            metrics_.messages_received.fetch_add(1);
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto latency_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
            
            metrics_.messages_processed.fetch_add(1);
            metrics_.total_latency_ns.fetch_add(latency_ns);
            
            // Update max latency
            uint64_t current_max = metrics_.max_latency_ns.load();
            while (current_max < static_cast<uint64_t>(latency_ns) && 
                   !metrics_.max_latency_ns.compare_exchange_weak(current_max, latency_ns)) {
                // Retry if another thread updated max_latency_ns
            }
            
        } else {
            // Queue full - increment overrun counter
            metrics_.buffer_overruns.fetch_add(1);
            
            if (metrics_.buffer_overruns.load() % 1000 == 1) {
                std::ostringstream oss;
                oss << "Queue overrun detected. Queue utilization: " 
                    << data_queue_->utilization() * 100.0 << "%";
                if (error_callback_) {
                    error_callback_(oss.str());
                }
            }
        }
    }
}

// Convert fixed-point price to double
double DatabentoHandler::ConvertPrice(int64_t fixed_price) const {
    if (fixed_price == UNDEF_PRICE) {
        return 0.0;  // Undefined price
    }
    return static_cast<double>(fixed_price) / PRICE_SCALE;
}

// Async fetch worker
void DatabentoHandler::AsyncFetchWorker(
    const std::string& dataset,
    const std::vector<std::string>& symbols,
    const std::string& start_time,
    const std::string& end_time,
    const std::string& schema,
    databento::SType stype_in) {
    
    try {
        // Set fetching flag at the beginning of actual work
        is_fetching_ = true;
        FetchHistoricalBBO(dataset, symbols, start_time, end_time, schema, stype_in);
    } catch (const std::exception& e) {
        std::ostringstream oss;
        oss << "Async fetch failed: " << e.what();
        if (error_callback_) {
            error_callback_(oss.str());
        }
    }
    
    // Always set flag to false when done
    is_fetching_ = false;
}

} // namespace market_data