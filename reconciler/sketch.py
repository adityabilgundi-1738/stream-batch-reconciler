from __future__ import annotations

import hashlib
import math
from collections import deque
from typing import Deque


class HyperLogLog:
    """O(1) memory probabilistic cardinality estimator (Flajolet et al.)."""

    def __init__(self, precision: int = 10) -> None:
        if not (4 <= precision <= 16):
            raise ValueError("precision must be between 4 and 16")
        self.precision = precision
        self.m = 1 << precision
        self.registers = bytearray(self.m)
        if self.m == 16:
            self.alpha = 0.673
        elif self.m == 32:
            self.alpha = 0.697
        elif self.m == 64:
            self.alpha = 0.709
        else:
            self.alpha = 0.7213 / (1.0 + 1.079 / self.m)

    def add(self, value: str) -> None:
        digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
        x = int.from_bytes(digest, "big", signed=False)
        idx = x >> (64 - self.precision)
        w = ((x << self.precision) & ((1 << 64) - 1)) | (1 << (self.precision - 1))
        leading_zeros = 64 - w.bit_length()
        rho = leading_zeros + 1
        if rho > self.registers[idx]:
            self.registers[idx] = rho

    def count(self) -> int:
        indicator = sum(2.0 ** (-r) for r in self.registers)
        raw_estimate = self.alpha * (self.m**2) / indicator

        if raw_estimate <= 2.5 * self.m:
            zero_registers = self.registers.count(0)
            if zero_registers > 0:
                return int(round(self.m * math.log(self.m / zero_registers)))

        return int(round(raw_estimate))

    @property
    def memory_bytes(self) -> int:
        return len(self.registers)

    @property
    def theoretical_std_error_pct(self) -> float:
        return round((1.04 / math.sqrt(self.m)) * 100.0, 2)


class AdaptiveWatermarkTracker:
    """Dynamically tracks p99 event-time vs. arrival-time lag."""

    def __init__(
        self,
        max_samples: int = 2048,
        quantile: float = 0.99,
        min_lateness_sec: float = 1.0,
        max_lateness_sec: float = 15.0,
    ) -> None:
        self.samples: Deque[float] = deque(maxlen=max_samples)
        self.quantile = quantile
        self.min_lateness_sec = min_lateness_sec
        self.max_lateness_sec = max_lateness_sec

    def observe(self, event_time_sec: float, arrival_time_sec: float) -> None:
        lag = max(0.0, arrival_time_sec - event_time_sec)
        self.samples.append(lag)

    def allowed_lateness_sec(self) -> float:
        if len(self.samples) < 32:
            return (self.min_lateness_sec + self.max_lateness_sec) / 2.0
        sorted_lags = sorted(self.samples)
        idx = min(
            len(sorted_lags) - 1,
            max(0, int(round(self.quantile * (len(sorted_lags) - 1)))),
        )
        p_lag = sorted_lags[idx]
        return max(self.min_lateness_sec, min(self.max_lateness_sec, p_lag * 1.15))
