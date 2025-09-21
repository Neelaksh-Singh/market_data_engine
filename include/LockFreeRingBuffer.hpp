#pragma once

#include "Types.hpp"
#include <atomic>
#include <array>
#include <memory>
#include <thread>

namespace market_data {

/**
 * Multi-Producer Multi-Consumer Lock-Free Queue
 * 
 * Maintains the same interface as the original LockFreeRingBuffer but now supports
 * multiple producers and consumers. Uses sequence numbers and CAS operations.
 * 
 * Template parameters:
 * - T: Type of elements to store (MarketDataPoint in our case)
 * - N: Buffer size (must be power of 2 for efficient modulo with bitwise AND)
 */
template<typename T, size_t N>
class LockFreeRingBuffer {
    static_assert((N & (N - 1)) == 0, "Buffer size must be power of 2");
    static_assert(N >= 2, "Buffer size must be at least 2");
    
private:
    // Each slot contains data + sequence number for MPMC coordination
    struct alignas(64) Slot {  // Cache line aligned to prevent false sharing
        std::atomic<std::size_t> sequence{0};
        T data{};
    };
    
    // Cache line separation to prevent false sharing between producer/consumer positions
    alignas(64) std::array<Slot, N> buffer_;
    alignas(64) std::atomic<std::size_t> enqueue_pos_{0};  // Producer position
    alignas(64) std::atomic<std::size_t> dequeue_pos_{0};  // Consumer position
    
    static constexpr std::size_t MASK = N - 1;  // For efficient modulo operation
    
public:
    LockFreeRingBuffer() {
        // Initialize sequence numbers - each slot starts with its index
        for (std::size_t i = 0; i < N; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }
    
    // Non-copyable to prevent accidental copies
    LockFreeRingBuffer(const LockFreeRingBuffer&) = delete;
    LockFreeRingBuffer& operator=(const LockFreeRingBuffer&) = delete;
    
    /**
     * Attempt to push an item to the buffer (multi-producer safe).
     * Returns true if successful, false if buffer is full.
     * 
     * Multiple producer threads can call this concurrently.
     */
    bool try_push(const T& item) {
        Slot* slot;
        std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        
        while (true) {
            slot = &buffer_[pos & MASK];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
            
            if (diff == 0) {
                // This slot is available for writing
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break; // Successfully claimed this slot
                }
                // CAS failed, another thread got this slot, retry with updated pos
            }
            else if (diff < 0) {
                // Queue is full
                return false;
            }
            else {
                // Another thread is working on this slot, update pos and retry
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }
        
        // Write data to claimed slot
        slot->data = item;
        
        // Release the slot for consumers (increment sequence to pos + 1)
        slot->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }
    
    /**
     * Attempt to pop an item from the buffer (multi-consumer safe).
     * Returns true if successful, false if buffer is empty.
     * 
     * Multiple consumer threads can call this concurrently.
     */
    bool try_pop(T& item) {
        Slot* slot;
        std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        
        while (true) {
            slot = &buffer_[pos & MASK];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
            
            if (diff == 0) {
                // This slot contains data ready for reading
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break; // Successfully claimed this slot
                }
                // CAS failed, another thread got this slot, retry with updated pos
            }
            else if (diff < 0) {
                // Queue is empty
                return false;
            }
            else {
                // Another thread is working on this slot, update pos and retry
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }
        
        // Read data from claimed slot
        item = slot->data;
        
        // Release the slot for producers (increment sequence to pos + N)
        slot->sequence.store(pos + N, std::memory_order_release);
        return true;
    }
    
    /**
     * Get current buffer utilization (0.0 to 1.0).
     * Useful for monitoring but not guaranteed to be exact due to concurrent access.
     */
    double utilization() const {
        std::size_t enq_pos = enqueue_pos_.load(std::memory_order_relaxed);
        std::size_t deq_pos = dequeue_pos_.load(std::memory_order_relaxed);
        return static_cast<double>(enq_pos - deq_pos) / N;
    }
    
    /**
     * Get current number of items in buffer.
     * Not guaranteed to be exact due to concurrent access.
     */
    size_t size() const {
        std::size_t enq_pos = enqueue_pos_.load(std::memory_order_relaxed);
        std::size_t deq_pos = dequeue_pos_.load(std::memory_order_relaxed);
        return static_cast<size_t>(enq_pos - deq_pos);
    }
    
    bool empty() const {
        return size() == 0;
    }
    
    static constexpr size_t capacity() {
        return N - 1;  // One slot reserved to distinguish full from empty
    }
};

} // namespace market_data