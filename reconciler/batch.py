from __future__ import annotations

import sqlite3
from typing import Dict, Iterable, List

from .models import Event, WindowAggregate


class BatchDataStore:
    def __init__(self, db_path: str = ":memory:") -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_events (
                event_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                event_time_sec REAL NOT NULL,
                partition_key TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_raw_events_time ON raw_events(event_time_sec);"
        )
        self.conn.commit()

    def append_batch(self, events: Iterable[Event]) -> int:
        rows = [
            (
                ev.event_id,
                ev.user_id,
                ev.amount_cents,
                ev.event_time_sec,
                ev.partition_key,
            )
            for ev in events
        ]
        with self.conn:
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO raw_events
                (event_id, user_id, amount_cents, event_time_sec, partition_key)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def fetch_all_events(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT event_id, user_id, amount_cents, event_time_sec FROM raw_events"
        ).fetchall()

    def close(self) -> None:
        self.conn.close()


class BatchAggregatorJob:
    def __init__(self, store: BatchDataStore, window_size_sec: float = 10.0) -> None:
        self.store = store
        self.window_size_sec = window_size_sec

    def run(self) -> Dict[int, WindowAggregate]:
        rows = self.store.fetch_all_events()
        windows: Dict[int, WindowAggregate] = {}
        for r in rows:
            event_time = float(r["event_time_sec"])
            window_id = int(event_time // self.window_size_sec)
            agg = windows.get(window_id)
            if agg is None:
                w_start = window_id * self.window_size_sec
                agg = WindowAggregate(
                    window_id=window_id,
                    window_start_sec=w_start,
                    window_end_sec=w_start + self.window_size_sec,
                )
                windows[window_id] = agg
            agg.event_count += 1
            agg.amount_sum_cents += int(r["amount_cents"])
            agg.user_ids.add(str(r["user_id"]))
        return windows
