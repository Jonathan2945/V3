#!/usr/bin/env python3

with open('intelligent_trading_engine.py', 'r') as f:
    content = f.read()

# Find and replace all problematic f-strings in the __init__ method
# The GitHub version is cleaner but still has format issues

# Wrap the entire _initialize_v3_binance_client_direct in try-except
old_method = '''    def _initialize_v3_binance_client_direct(self):
        """Initialize V3 Binance client with DIRECT credential loading - NO API ROTATION"""
        try:'''

new_method = '''    def _initialize_v3_binance_client_direct(self):
        """Initialize V3 Binance client with DIRECT credential loading - NO API ROTATION"""
        try:
            import logging
            logger = logging.getLogger(__name__)'''

content = content.replace(old_method, new_method)

# Find and comment out ALL print statements with f-strings in this method
lines = content.split('\n')
output_lines = []
in_init_method = False

for i, line in enumerate(lines):
    if 'def _initialize_v3_binance_client_direct(self)' in line:
        in_init_method = True
        output_lines.append(line)
    elif in_init_method and line.strip().startswith('def ') and '__init__' not in line:
        in_init_method = False
        output_lines.append(line)
    elif in_init_method and 'print(f"' in line:
        # Comment out this line
        indent = len(line) - len(line.lstrip())
        output_lines.append(' ' * indent + '# ' + line.lstrip() + '  # Disabled to prevent format errors')
    else:
        output_lines.append(line)

content = '\n'.join(output_lines)

# Also add a simple success log at the end of the method
content = content.replace(
    'return True\n            \n        except Exception as e:\n            logging.error(f"V3 Binance client initialization failed: {e}")',
    'logger.info("[V3_ENGINE] Binance client initialized successfully")\n            return True\n            \n        except Exception as e:\n            logging.error(f"V3 Binance client initialization failed: {e}")'
)

with open('intelligent_trading_engine.py', 'w') as f:
    f.write(content)

print("✅ Fixed GitHub version of intelligent_trading_engine.py")
