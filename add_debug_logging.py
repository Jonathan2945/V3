#!/usr/bin/env python3

with open('main_controller.py', 'r') as f:
    content = f.read()

# Find _update_real_time_data and add debug logging
old_update = '''    async def _update_real_time_data(self):
        """Update real-time data and execute trading"""
        try:
            self.system_resources['cpu_usage'] = psutil.cpu_percent(interval=0.1)'''

new_update = '''    async def _update_real_time_data(self):
        """Update real-time data and execute trading"""
        try:
            self.logger.debug("[UPDATE] _update_real_time_data called")
            self.system_resources['cpu_usage'] = psutil.cpu_percent(interval=0.1)'''

content = content.replace(old_update, new_update)

# Add debug logging to _scan_real_market_opportunities
old_scan = '''    async def _scan_real_market_opportunities(self):
        """Scan real market for opportunities"""
        try:
            if not self.trading_engine or not hasattr(self.trading_engine, 'client'):'''

new_scan = '''    async def _scan_real_market_opportunities(self):
        """Scan real market for opportunities"""
        try:
            self.logger.info("[SCAN] Scanning market for opportunities...")
            if not self.trading_engine or not hasattr(self.trading_engine, 'client'):
                self.logger.warning("[SCAN] No trading engine or client available")'''

content = content.replace(old_scan, new_scan)

# Add debug logging to _execute_real_trading_logic
old_execute = '''    async def _execute_real_trading_logic(self):
        """Execute real trading logic"""
        try:
            if self.scanner_data['opportunities'] == 0:'''

new_execute = '''    async def _execute_real_trading_logic(self):
        """Execute real trading logic"""
        try:
            self.logger.info("[TRADE] Execute trading logic called")
            if self.scanner_data['opportunities'] == 0:
                self.logger.info("[TRADE] No opportunities found")'''

content = content.replace(old_execute, new_execute)

with open('main_controller.py', 'w') as f:
    f.write(content)

print("✅ Added debug logging to trading functions")
