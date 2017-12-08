#!/usr/bin/env python3
"""
Streaming SQL parser and tumbling/sliding window aggregator.
Author: Yousef Khaled (Distributed Systems Architecture - 2014)
"""

import time
import json
from collections import defaultdict
from typing import List, Dict, Any

class WindowAggregator:
    def __init__(self, window_size_sec: int = 10):
        self.window_size_sec = window_size_sec
        self.buckets: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

    def ingest(self, record: Dict[str, Any]):
        timestamp = record.get("timestamp", int(time.time()))
        bucket_key = (timestamp // self.window_size_sec) * self.window_size_sec
        self.buckets[bucket_key].append(record)

    def evaluate_windows(self):
        current_time = int(time.time())
        results = []
        for bucket_time, records in list(self.buckets.items()):
            if current_time - bucket_time >= self.window_size_sec:
                # Window closed, emit aggregation
                count = len(records)
                avg_val = sum(r.get("value", 0) for r in records) / max(count, 1)
                results.append({
                    "window_start": bucket_time,
                    "window_end": bucket_time + self.window_size_sec,
                    "event_count": count,
                    "average_value": round(avg_val, 2)
                })
                del self.buckets[bucket_time]
        return results

if __name__ == "__main__":
    print("[SQL ENGINE] Streaming Tumbling Window Engine started (Window: 10s)...")
    agg = WindowAggregator(window_size_sec=10)
    for i in range(50):
        agg.ingest({"device_id": f"dev_{i%5}", "value": i * 1.5, "timestamp": int(time.time()) - 15})
    emitted = agg.evaluate_windows()
    print(f"[SQL ENGINE] Emitted {len(emitted)} window aggregations: {emitted}")
