# market_data_engine
Low-latency system that consumes live market data from Databento, passes it through a lock-free ring buffer, and processes it in a separate thread to compute a simple real-time statistic.
