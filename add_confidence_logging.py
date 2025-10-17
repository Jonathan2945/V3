#!/usr/bin/env python3

with open('main_controller.py', 'r') as f:
    content = f.read()

# Find _analyze_pair_with_strategies and add logging
import re

# Add logging after confidence calculation
pattern = r'(return total_confidence / max\(strategy_count, 1\))'
replacement = r'''confidence_result = total_confidence / max(strategy_count, 1)
            self.logger.info(f"[CONFIDENCE DEBUG] {pair}: {confidence_result:.1f}% from {strategy_count} strategies")
            return confidence_result'''

content = re.sub(pattern, replacement, content)

# Also add logging in the loop
pattern2 = r'(total_confidence \+= confidence\s+strategy_count \+= 1)'
replacement2 = r'''total_confidence += confidence
                strategy_count += 1
                self.logger.info(f"[STRATEGY] {strategy_type}: confidence={confidence:.1f}%")'''

content = re.sub(pattern2, replacement2, content)

with open('main_controller.py', 'w') as f:
    f.write(content)

print("✓ Added confidence debug logging")
