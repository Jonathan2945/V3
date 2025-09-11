#!/usr/bin/env python3
import sys
try:
    import progressbar2
    print("SUCCESS: progressbar2 imported")
    print(f"Location: {progressbar2.__file__}")
    print(f"Version: {getattr(progressbar2, '__version__', 'unknown')}")
    
    # Test basic functionality
    import time
    bar = progressbar2.ProgressBar(max_value=10)
    for i in range(11):
        time.sleep(0.1)
        bar.update(i)
    bar.finish()
    print("SUCCESS: progressbar2 basic test completed")
    
except ImportError as e:
    print(f"FAILED: Cannot import progressbar2: {e}")
    sys.exit(1)
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
