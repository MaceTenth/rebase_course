from typing import Callable, Optional
from datetime import datetime, timedelta
from collections import deque


class Monitor:
    def __init__(self, failure_threshold: int, window_seconds: int = 60, alert_handler: Optional[Callable[[str], None]] = None):
        """
        Initialize the monitor with a failure threshold and optional alert handler.
        
        Args:
            failure_threshold: Number of consecutive failures before raising an alert
            window_seconds: Time window in seconds to check for failures
            alert_handler: Optional callback function to handle alerts. If None, prints to stdout
        """
        if failure_threshold <= 0:
            raise ValueError("Failure threshold must be positive")
        # if window_seconds < 60:
        #     raise ValueError("Window seconds must be >= 60")
            
        self._failure_threshold = failure_threshold
        self._window_seconds = window_seconds
        self._alert_handler = alert_handler or self._default_alert_handler
        self._total_passes = 0
        self._total_failures = 0
        self._failure_timestamps = deque()  # Store failure timestamps
        self._last_status_time = datetime.now()

    def _clean_old_failures(self) -> None:
        """Remove failures outside the time window."""
        now = datetime.now()
        window_start = now - timedelta(seconds=self._window_seconds)
        
        while self._failure_timestamps and self._failure_timestamps[0] < window_start:
            self._failure_timestamps.popleft()

    def _default_alert_handler(self, message: str) -> None:
        """Default alert handler that prints to stdout."""
        print(f"[ALERT] {message}")

    def pass_(self) -> None:
        """Record a successful action."""
        self._total_passes += 1
        self._last_status_time = datetime.now()
        self._clean_old_failures()

    def fail(self) -> None:
        """
        Record a failed action.
        Triggers alert if consecutive failures reach threshold within the time window.
        """
        now = datetime.now()
        self._failure_timestamps.append(now)
        self._total_failures += 1
        self._last_status_time = now
        
        self._clean_old_failures()
        
        if len(self._failure_timestamps) == self._failure_threshold:
            self._alert_handler(
                f"Alert: {self._failure_threshold} consecutive failures detected within {self._window_seconds}s! "
                f"(Total passes: {self._total_passes}, Total failures: {self._total_failures})"
            )

    @property
    def consecutive_failures(self) -> int:
        """Get current number of consecutive failures within the window."""
        self._clean_old_failures()
        return len(self._failure_timestamps)

    @property
    def stats(self) -> dict:
        """Get monitor statistics."""
        self._clean_old_failures()
        return {
            'total_passes': self._total_passes,
            'total_failures': self._total_failures,
            'consecutive_failures': len(self._failure_timestamps),
            'last_status_time': int(self._last_status_time.timestamp()),
            'window_seconds': self._window_seconds
        }



def custom_alert(message: str):
    print("\n" + "=" * 50)
    print(f"🚨 Marvin Alert 🚨\n{message}")
    print("Here I am, brain the size of a planet, and they ask me to handle alerts. Pathetic.")
    print("=" * 50 + "\n")


def test_monitor():
    # Test with 3 second window for demonstration
    monitor = Monitor(failure_threshold=3, window_seconds=3)
    
    print("\n=== Default monitor ===")
    monitor.pass_()
    print("After first pass:", monitor.stats)
    
    monitor.fail()
    print("After 1st failure:", monitor.stats)
    monitor.fail()
    print("After 2nd failure:", monitor.stats)
    monitor.fail()
    print("After 3rd failure:", monitor.stats)
    
    # Wait 4 seconds to exceed window
    import time
    time.sleep(4)
    print("After waiting 4 seconds:", monitor.stats)
    
    monitor.fail()
    print("After new failure:", monitor.stats)

if __name__ == "__main__":
    test_monitor()