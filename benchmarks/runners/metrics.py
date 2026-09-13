import os
import threading
import time

from contextlib import contextmanager
from dataclasses import dataclass
from statistics import mean, median

try:
    import psutil  # type: ignore[import-untyped]

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


RSS_PEAK_SAMPLING_INTERVAL_MS = 5.0


@dataclass
class Snapshot:
    wall_time: float
    cpu_user: float
    cpu_sys: float
    rss_memory: int

    def delta(
        self,
        other: "Snapshot",
        *,
        rss_peak_bytes: int | None = None,
    ) -> "Metrics":
        return Metrics(
            wall_time_sec=self.wall_time - other.wall_time,
            cpu_user_sec=self.cpu_user - other.cpu_user,
            cpu_sys_sec=self.cpu_sys - other.cpu_sys,
            rss_delta_bytes=self.rss_memory - other.rss_memory,
            rss_peak_bytes=max(
                self.rss_memory,
                other.rss_memory,
                rss_peak_bytes or 0,
            ),
        )


class _RssPeakSampler:
    def __init__(self, interval_ms: float = RSS_PEAK_SAMPLING_INTERVAL_MS) -> None:
        self._interval_seconds = interval_ms / 1000
        self._stop = threading.Event()
        self._process = psutil.Process(os.getpid()) if HAS_PSUTIL else None
        self._peak_bytes = self._read_rss()
        self._thread = threading.Thread(
            target=self._sample,
            name="mongoeco-benchmark-rss",
            daemon=True,
        )

    def _read_rss(self) -> int:
        if self._process is None:
            return 0
        try:
            return int(self._process.memory_info().rss)
        except (OSError, RuntimeError):
            return 0

    def _sample(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            self._peak_bytes = max(self._peak_bytes, self._read_rss())

    def start(self) -> None:
        if self._process is not None:
            self._thread.start()

    def stop(self) -> int:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join()
        self._peak_bytes = max(self._peak_bytes, self._read_rss())
        return self._peak_bytes


@dataclass
class Metrics:
    wall_time_sec: float
    cpu_user_sec: float
    cpu_sys_sec: float
    rss_delta_bytes: int
    rss_peak_bytes: int

    def to_dict(self) -> dict[str, object]:
        return {
            "wall_time_sec": round(self.wall_time_sec, 4),
            "cpu_user_sec": round(self.cpu_user_sec, 4),
            "cpu_sys_sec": round(self.cpu_sys_sec, 4),
            "rss_delta_mb": round(self.rss_delta_bytes / (1024 * 1024), 2),
            "rss_peak_mb": round(self.rss_peak_bytes / (1024 * 1024), 2),
        }


@dataclass
class MetricsSummary:
    repetitions: int
    wall_time_mean_sec: float
    wall_time_median_sec: float
    wall_time_min_sec: float
    wall_time_max_sec: float
    cpu_user_mean_sec: float
    cpu_sys_mean_sec: float
    rss_delta_mean_mb: float
    rss_peak_max_mb: float

    def to_dict(self) -> dict[str, object]:
        return {
            "repetitions": self.repetitions,
            "wall_time_mean_sec": round(self.wall_time_mean_sec, 4),
            "wall_time_median_sec": round(self.wall_time_median_sec, 4),
            "wall_time_min_sec": round(self.wall_time_min_sec, 4),
            "wall_time_max_sec": round(self.wall_time_max_sec, 4),
            "cpu_user_mean_sec": round(self.cpu_user_mean_sec, 4),
            "cpu_sys_mean_sec": round(self.cpu_sys_mean_sec, 4),
            "rss_delta_mean_mb": round(self.rss_delta_mean_mb, 2),
            "rss_peak_max_mb": round(self.rss_peak_max_mb, 2),
        }


def get_snapshot() -> Snapshot:
    t = os.times()
    rss = 0
    if HAS_PSUTIL:
        process = psutil.Process(os.getpid())
        rss = process.memory_info().rss

    return Snapshot(
        wall_time=time.perf_counter(),
        cpu_user=t.user,
        cpu_sys=t.system,
        rss_memory=rss,
    )


@contextmanager
def measure(result_box: list[Metrics]):
    """
    Context manager to measure execution block.
    Stores the result in result_box (a list) to return the metrics out of the scope.
    """
    sampler = _RssPeakSampler()
    sampler.start()
    start = get_snapshot()
    try:
        yield
    finally:
        end = get_snapshot()
        result_box.append(
            end.delta(start, rss_peak_bytes=sampler.stop()),
        )


def summarize(metrics: list[Metrics]) -> MetricsSummary:
    if not metrics:
        raise ValueError("at least one metric sample is required")
    return MetricsSummary(
        repetitions=len(metrics),
        wall_time_mean_sec=mean(item.wall_time_sec for item in metrics),
        wall_time_median_sec=median(item.wall_time_sec for item in metrics),
        wall_time_min_sec=min(item.wall_time_sec for item in metrics),
        wall_time_max_sec=max(item.wall_time_sec for item in metrics),
        cpu_user_mean_sec=mean(item.cpu_user_sec for item in metrics),
        cpu_sys_mean_sec=mean(item.cpu_sys_sec for item in metrics),
        rss_delta_mean_mb=mean(item.rss_delta_bytes for item in metrics)
        / (1024 * 1024),
        rss_peak_max_mb=max(item.rss_peak_bytes for item in metrics) / (1024 * 1024),
    )
