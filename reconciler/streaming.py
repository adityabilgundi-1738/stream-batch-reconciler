from __future__ import annotations

import hashlib
import random
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, Iterator, List, Optional, Set

from .models import AnomalyType, Event, WindowAggregate
from .sketch import AdaptiveWatermarkTracker, HyperLogLog


@dataclass
class WindowFaultCounter:
    dropped: int = 0
    duplicated: int = 0
    late: int = 0

    @property
    def expected_anomaly(self) -> AnomalyType:
        loss = self.dropped + self.late
        if loss > 0 and self.duplicated == 0:
            return AnomalyType.MESSAGE_LOSS_OR_LATE
        if self.duplicated > 0 and loss == 0:
            return AnomalyType.DUPLICATE_DELIVERY
        if loss > 0 and self.duplicated > 0:
            return AnomalyType.MIXED_DRIFT
        return AnomalyType.NONE


@dataclass
class FaultInjectionStats:
    dropped_events: int = 0
    duplicated_events: int = 0
    late_events: int = 0
    corrupted_windows: Set[int] = field(default_factory=set)
    window_faults: Dict[int, WindowFaultCounter] = field(default_factory=dict)


class PartitionedStreamBroker:
    def __init__(
        self,
        num_partitions: int = 8,
        window_size_sec: float = 10.0,
        drop_rate: float = 0.0,
        duplicate_rate: float = 0.0,
        late_arrival_rate: float = 0.0,
        late_delay_sec: float = 25.0,
        corrupted_window_Filter: Optional[Set[int]] = None,
        window_anomaly_mode: Optional[Dict[int, AnomalyType]] = None,
        seed: int = 42,
    ) -> None:
        self.num_partitions = num_partitions
        self.window_size_sec = window_size_sec
        self.drop_rate = drop_rate
        self.duplicate_rate = duplicate_rate
        self.late_arrival_rate = late_arrival_rate
        self.late_delay_sec = late_delay_sec
        self.corrupted_window_filter = corrupted_window_Filter
        self.window_anomaly_mode = window_anomaly_mode
        self._rng = random.Random(seed)
        self.partitions: List[List[Event]] = [[] for _ in range(num_partitions)]
        self.stats = FaultInjectionStats()

    def _partition_for(self, key: str) -> int:
        digest = hashlib.md5(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:4], "little") % self.num_partitions

    def _wf(self, window_id: int) -> WindowFaultCounter:
        wf = self.stats.window_faults.get(window_id)
        if wf is None:
            wf = WindowFaultCounter()
            self.stats.window_faults[window_id] = wf
        return wf

    def publish(self, event: Event) -> None:
        window_id = int(event.event_time_sec // self.window_size_sec)
        inject_faults = (
            self.corrupted_window_filter is None
            or window_id in self.corrupted_window_filter
        )

        mode = (
            self.window_anomaly_mode.get(window_id)
            if self.window_anomaly_mode is not None
            else None
        )
        eff_drop = self.drop_rate
        eff_dup = self.duplicate_rate
        eff_late = self.late_arrival_rate
        if mode == AnomalyType.DUPLICATE_DELIVERY:
            eff_drop = 0.0
            eff_late = 0.0
        elif mode == AnomalyType.MESSAGE_LOSS_OR_LATE:
            eff_dup = 0.0

        p_idx = self._partition_for(event.partition_key)

        if inject_faults and eff_drop > 0 and self._rng.random() < eff_drop:
            self.stats.dropped_events += 1
            self.stats.corrupted_windows.add(window_id)
            self._wf(window_id).dropped += 1
            return

        delivered_event = event
        if inject_faults and eff_late > 0 and self._rng.random() < eff_late:
            delivered_event = Event(
                event_id=event.event_id,
                user_id=event.user_id,
                amount_cents=event.amount_cents,
                event_time_sec=event.event_time_sec,
                arrival_time_sec=event.arrival_time_sec + self.late_delay_sec,
                partition_key=event.partition_key,
            )
            self.stats.late_events += 1
            self.stats.corrupted_windows.add(window_id)
            self._wf(window_id).late += 1

        self.partitions[p_idx].append(delivered_event)

        if inject_faults and eff_dup > 0 and self._rng.random() < eff_dup:
            self.partitions[p_idx].append(delivered_event)
            self.stats.duplicated_events += 1
            self.stats.corrupted_windows.add(window_id)
            self._wf(window_id).duplicated += 1

    def consume_all(self) -> Iterator[Event]:
        for partition in self.partitions:
            for ev in partition:
                yield ev


class StreamingConsumer:
    def __init__(
        self,
        window_size_sec: float = 10.0,
        allowed_lateness_sec: float = 5.0,
        deduplicate: bool = False,
        use_hll: bool = False,
        hll_precision: int = 10,
        adaptive_watermark: bool = False,
    ) -> None:
        self.window_size_sec = window_size_sec
        self.allowed_lateness_sec = allowed_lateness_sec
        self.deduplicate = deduplicate
        self.use_hll = use_hll
        self.hll_precision = hll_precision
        self.adaptive_watermark = adaptive_watermark
        self.watermark_tracker = AdaptiveWatermarkTracker(
            min_lateness_sec=allowed_lateness_sec,
            max_lateness_sec=allowed_lateness_sec * 3.0,
        )
        self.serving_store: Dict[int, WindowAggregate] = {}
        self._seen_event_ids: Set[str] = set()
        self.max_observed_time_sec: float = 0.0
        self.processed_events: int = 0
        self.dropped_late_events: int = 0
        self.processing_duration_sec: float = 0.0

    def process_stream(self, events: Iterable[Event]) -> float:
        start_ns = time.perf_counter_ns()
        for ev in events:
            self._ingest_one(ev)
        elapsed_sec = max(1e-9, (time.perf_counter_ns() - start_ns) / 1_000_000_000.0)
        self.processing_duration_sec += elapsed_sec
        return self.throughput_eps

    def _ingest_one(self, ev: Event) -> None:
        self.processed_events += 1
        if ev.event_time_sec > self.max_observed_time_sec:
            self.max_observed_time_sec = ev.event_time_sec

        if self.deduplicate:
            if ev.event_id in self._seen_event_ids:
                return
            self._seen_event_ids.add(ev.event_id)

        window_id = int(ev.event_time_sec // self.window_size_sec)
        window_start = window_id * self.window_size_sec
        window_end = window_start + self.window_size_sec

        eff_lateness = (
            self.watermark_tracker.allowed_lateness_sec()
            if self.adaptive_watermark
            else self.allowed_lateness_sec
        )
        if ev.arrival_time_sec > (window_end + eff_lateness):
            self.dropped_late_events += 1
            return

        if self.adaptive_watermark:
            self.watermark_tracker.observe(ev.event_time_sec, ev.arrival_time_sec)

        agg = self.serving_store.get(window_id)
        if agg is None:
            agg = WindowAggregate(
                window_id=window_id,
                window_start_sec=window_start,
                window_end_sec=window_end,
                hll=HyperLogLog(self.hll_precision) if self.use_hll else None,
            )
            self.serving_store[window_id] = agg

        agg.event_count += 1
        agg.amount_sum_cents += ev.amount_cents
        if agg.hll is not None:
            agg.hll.add(ev.user_id)
        else:
            agg.user_ids.add(ev.user_id)

    @property
    def throughput_eps(self) -> float:
        if self.processing_duration_sec <= 0:
            return 0.0
        return self.processed_events / self.processing_duration_sec

    @property
    def cardinality_memory_bytes_per_window(self) -> int:
        if not self.serving_store:
            return 0
        sample = next(iter(self.serving_store.values()))
        if sample.hll is not None:
            return sample.hll.memory_bytes
        return len(sample.user_ids) * 64
