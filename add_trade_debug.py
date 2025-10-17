#!/usr/bin/env python3

with open('main_controller.py', 'r') as f:
    lines = f.readlines()

# Find _execute_real_trading_logic and add debug logging
new_lines = []
for i, line in enumerate(lines):
    new_lines.append(line)
    
    if 'async def _execute_real_trading_logic' in line:
        # Add logging after function start
        new_lines.append('        """Execute real trading logic"""\n')
        new_lines.append('        self.logger.info(f"[TRADE DEBUG] scanner_data: {self.scanner_data}")\n')
        new_lines.append('        self.logger.info(f"[TRADE DEBUG] is_trading: {self.is_trading}")\n')
        
    # Log before early returns
    if "if self.scanner_data['opportunities'] == 0:" in line and i > 0 and 'execute_real_trading_logic' in ''.join(lines[max(0,i-10):i]):
        indent = line[:len(line) - len(line.lstrip())]
        new_lines.insert(-1, f'{indent}self.logger.info("[TRADE DEBUG] Checking opportunities...")\n')

with open('main_controller.py', 'w') as f:
    f.writelines(new_lines)

print("✓ Added trade execution debug logging")
