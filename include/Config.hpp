#pragma once
#include <string>
#include <vector>

namespace config {

// === Queue Parameters ===
inline constexpr size_t QUEUE_SIZE = 1024 * 1024;  // 1M slots

// === Databento Parameters ===
inline const std::string DATASET = "GLBX.MDP3";  
inline const std::vector<std::string> SYMBOLS = {"ES.FUT", "NQ.FUT", "YM.FUT"};  
inline const std::string START_TIME = "2022-06-10T14:30:00";  
inline const std::string END_TIME   = "2022-06-10T14:35:00";  
inline const std::string SCHEMA     = "bbo-1s";  
inline constexpr int FETCH_TIMEOUT_SECONDS = 30;

// === Logging Parameters ===
inline constexpr bool ENABLE_SAMPLE_OUTPUT = true;
inline constexpr size_t SAMPLE_PRINT_EVERY = 1000;

} // namespace config
