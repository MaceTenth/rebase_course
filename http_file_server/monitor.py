from typing import Callable, Optional
from datetime import datetime


class Monitor:
    def __init__(self, failure_threshold: int, alert_handler: Optional[Callable[[str], None]] = None):
        """
        Initialize the monitor with a failure threshold and optional alert handler.
        
        Args:
            failure_threshold: Number of consecutive failures before raising an alert
            alert_handler: Optional callback function to handle alerts. If None, prints to stdout
        """
        if failure_threshold <= 0:
            raise ValueError("Failure threshold must be positive")
            
        self._failure_threshold = failure_threshold
        self._consecutive_failures = 0
        self._alert_handler = alert_handler or self._default_alert_handler
        self._total_passes = 0
        self._total_failures = 0
        self._last_status_time = datetime.now()

    def _default_alert_handler(self, message: str) -> None:
        """Default alert handler that prints to stdout."""
        print(f"[ALERT] {message}")

    def pass_(self) -> None:
        """Record a successful action."""
        self._consecutive_failures = 0
        self._total_passes += 1
        self._last_status_time = datetime.now()

    def fail(self) -> None:
        """
        Record a failed action.
        Triggers alert if consecutive failures reach threshold.
        """
        self._consecutive_failures += 1
        self._total_failures += 1
        self._last_status_time = datetime.now()

        if self._consecutive_failures == self._failure_threshold:
            self._alert_handler(
                f"Alert: {self._failure_threshold} consecutive failures detected! "
                f"(Total passes: {self._total_passes}, Total failures: {self._total_failures})"
            )

    @property
    def consecutive_failures(self) -> int:
        """Get current number of consecutive failures."""
        return self._consecutive_failures

    @property
    def stats(self) -> dict:
        """Get monitor statistics."""
        return {
            'total_passes': self._total_passes,
            'total_failures': self._total_failures,
            'consecutive_failures': self._consecutive_failures,
            'last_status_time': int(self._last_status_time.timestamp()) 
        }



def custom_alert(message: str):
    print("\n" + "=" * 50)
    print(f"🚨 Marvin Alert 🚨\n{message}")
    print("Here I am, brain the size of a planet, and they ask me to handle alerts. Pathetic.")
    print("=" * 50 + "\n")


def test_monitor():
    
    monitor = Monitor(failure_threshold=3)
    
    print("\n=== Default monitor ===")
    monitor.pass_()  
    print("After first pass:", monitor.stats)
    
    monitor.fail()   
    monitor.fail()   
    monitor.fail()   
    print("After 3 failures:", monitor.stats)
    
    monitor.pass_()  # 
    print("After final pass:", monitor.stats)

    
    print("\n=== Custom monitor ===")
    monitor_custom = Monitor(failure_threshold=3, alert_handler=custom_alert)
    
    monitor_custom.pass_()  
    print("After first pass:", monitor_custom.stats)
    
    monitor_custom.fail()   
    monitor_custom.fail()   
    monitor_custom.fail()   
    print("After 3 failures:", monitor_custom.stats)
    
    monitor_custom.pass_()
    print("After final pass:", monitor_custom.stats)

if __name__ == "__main__":
    test_monitor()