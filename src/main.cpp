#include "../include/DatabentoHandler.hpp"
#include "../include/LockFreeRingBuffer.hpp"
#include "../include/Types.hpp"
#include "../include/Config.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <signal.h>

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
                    const PerformanceMetrics& metrics) {
    
    MarketDataPoint data_point;
    size_t processed_count = 0;
    auto last_report_time = std::chrono::steady_clock::now();
    
    while (running.load()) {
        if (queue.try_pop(data_point)) {
            processed_count++;
            
            // Print sample data every 1000 messages
            if (processed_count % 1000 == 1) {
                std::cout << "Sample data point " << processed_count << ":" << std::endl;
                std::cout << "  Instrument ID: " << data_point.instrument_id << std::endl;
                std::cout << "  Bid: " << data_point.bid_px << " @ " << data_point.bid_sz << std::endl;
                std::cout << "  Ask: " << data_point.ask_px << " @ " << data_point.ask_sz << std::endl;
                std::cout << "  Timestamp: " << data_point.timestamp_delta << std::endl;
                std::cout << std::endl;
            }
        } else {
            // No data available, small delay to prevent busy waiting
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        
        // Report metrics every 5 seconds
        auto now = std::chrono::steady_clock::now();
        if (now - last_report_time > std::chrono::seconds(5)) {
            std::cout << "=== Consumer Status Report ===" << std::endl;
            std::cout << "Processed: " << processed_count << std::endl;
            std::cout << "Queue size: " << queue.size() << std::endl;
            std::cout << "Queue utilization: " << queue.utilization() * 100.0 << "%" << std::endl;
            std::cout << "Messages received: " << metrics.messages_received.load() << std::endl;
            std::cout << "Buffer overruns: " << metrics.buffer_overruns.load() << std::endl;
            std::cout << "Avg latency: " << metrics.avg_latency_us() << " μs" << std::endl;
            std::cout << "Push success rate: " << metrics.push_success_rate() * 100.0 << "%" << std::endl;
            std::cout << "===============================" << std::endl << std::endl;
            
            last_report_time = now;
        }
    }
    
    std::cout << "Consumer thread exiting. Total processed: " << processed_count << std::endl;
}

int main() {
    // Set up signal handler
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    std::cout << "=== Databento MPMC Queue Demo ===" << std::endl;
    std::cout << "This demo fetches historical BBO data and processes it using" << std::endl;
    std::cout << "a multi-producer multi-consumer lock-free queue." << std::endl << std::endl;
    
    try {
        // Create Databento handler using environment variable for API key
        std::cout << "Creating Databento handler..." << std::endl;
        auto handler = DatabentoHandler::CreateFromEnv(config::QUEUE_SIZE);
        
        // Set error callback
        handler->SetErrorCallback([](const std::string& error) {
            std::cerr << "ERROR: " << error << std::endl;
        });
        
        // Start consumer thread
        std::cout << "Starting consumer thread..." << std::endl;
        std::thread consumer(consumer_thread, 
                           std::ref(handler->GetQueue()), 
                           std::ref(handler->GetMetrics()));
        
        // Configuration for data fetch
        std::string dataset = config::DATASET;
        auto symbols        = config::SYMBOLS;
        std::string start_time = config::START_TIME;
        std::string end_time   = config::END_TIME;
        std::string schema     = config::SCHEMA;
                
        std::cout << "Fetching historical data..." << std::endl;
        std::cout << "Dataset: " << dataset << std::endl;
        std::cout << "Symbols: ";
        for (const auto& sym : symbols) std::cout << sym << " ";
        std::cout << std::endl;
        std::cout << "Time range: " << start_time << " to " << end_time << std::endl;
        std::cout << "Schema: " << schema << std::endl << std::endl;
        
        // Start async fetch
        handler->StartAsyncFetch(dataset, symbols, start_time, end_time, schema);
        
        // Wait for fetch to complete or user interrupt
        int wait_count = 0;
        while (handler->IsFetching() && running.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            wait_count++;
            std::cout << "Waiting... " << wait_count << " seconds" << std::endl;
            
            if (wait_count > config::FETCH_TIMEOUT_SECONDS) { // Default timeout after 30 seconds
                std::cout << "Timeout waiting for data fetch" << std::endl;
                break;
            }
        }
        
        // If fetch completed, wait a bit more for consumer to process remaining data
        if (!handler->IsFetching()) {
            std::cout << "Fetch completed. Waiting for consumer to process remaining data..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
        
        // Stop consumer
        running = false;
        if (consumer.joinable()) {
            consumer.join();
        }
        
        // Final metrics report
        const auto& metrics = handler->GetMetrics();
        std::cout << "\n=== Final Metrics Report ===" << std::endl;
        std::cout << "Messages received: " << metrics.messages_received.load() << std::endl;
        std::cout << "Messages processed: " << metrics.messages_processed.load() << std::endl;
        std::cout << "Buffer overruns: " << metrics.buffer_overruns.load() << std::endl;
        std::cout << "Buffer underruns: " << metrics.buffer_underruns.load() << std::endl;
        std::cout << "Average latency: " << metrics.avg_latency_us() << " μs" << std::endl;
        std::cout << "Maximum latency: " << metrics.max_latency_ns.load() << " ns" << std::endl;
        std::cout << "Push success rate: " << metrics.push_success_rate() * 100.0 << "%" << std::endl;
        std::cout << "=============================" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    std::cout << "Demo completed successfully!" << std::endl;
    return 0;
}