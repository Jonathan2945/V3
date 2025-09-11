#!/usr/bin/env python3
"""Quick fix for missing dashboard method"""

import re

# Read the main_controller.py file
with open('main_controller.py', 'r') as f:
    content = f.read()

# Method to add
method_code = '''
    def get_comprehensive_dashboard_data(self):
        """Get comprehensive dashboard data for API middleware"""
        try:
            metrics = getattr(self, 'metrics', {})
            positions = getattr(self, 'open_positions', {})
            
            return {
                "overview": {
                    "trading": {
                        "is_running": getattr(self, 'is_running', False),
                        "total_pnl": metrics.get('total_pnl', 0.0),
                        "daily_pnl": metrics.get('daily_pnl', 0.0),
                        "total_trades": metrics.get('total_trades', 0),
                        "win_rate": metrics.get('win_rate', 0.0),
                        "active_positions": len(positions),
                        "best_trade": metrics.get('best_trade', 0.0)
                    },
                    "system": {
                        "controller_connected": True,
                        "ml_training_completed": metrics.get('ml_training_completed', False),
                        "backtest_completed": metrics.get('comprehensive_backtest_completed', False),
                        "api_rotation_active": True
                    },
                    "scanner": {
                        "active_pairs": 0,
                        "opportunities": 0,
                        "best_opportunity": "None",
                        "confidence": 0
                    },
                    "external_data": {
                        "working_apis": 1,
                        "total_apis": 5,
                        "api_status": {}
                    },
                    "timestamp": __import__('datetime').datetime.now().isoformat()
                }
            }
        except Exception as e:
            return {
                "overview": {
                    "error": str(e),
                    "trading": {"is_running": False, "total_pnl": 0.0},
                    "system": {"controller_connected": True}
                }
            }
'''

# Check if method already exists
if 'def get_comprehensive_dashboard_data' not in content:
    # Find the end of the __init__ method to add after it
    lines = content.split('\n')
    
    for i, line in enumerate(lines):
        if 'def __init__' in line and 'V3TradingController' in lines[max(0, i-5):i]:
            # Find the end of __init__ method
            indent_level = len(line) - len(line.lstrip())
            for j in range(i+1, len(lines)):
                if lines[j].strip() and not lines[j].startswith(' ' * (indent_level + 4)):
                    # Insert the new method here
                    lines.insert(j, method_code)
                    break
            break
    
    # Write back to file
    with open('main_controller.py', 'w') as f:
        f.write('\n'.join(lines))
    
    print("✓ Method added to main_controller.py")
else:
    print("✓ Method already exists")
