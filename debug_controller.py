import sys
import traceback
sys.path.append('.')

# Import required modules
from main_controller import V3TradingController

try:
    # Create controller without API manager first
    controller = V3TradingController()
    print("✓ Controller created successfully")
    
    # Test if method exists
    if hasattr(controller, 'get_comprehensive_dashboard_data'):
        print("✓ Method exists")
        
        # Try to call it
        try:
            result = controller.get_comprehensive_dashboard_data()
            print("✓ Method executed successfully")
            print("Result type:", type(result))
            if isinstance(result, dict):
                print("Keys:", list(result.keys()))
                if 'overview' in result:
                    print("✓ 'overview' key exists")
                    print("Overview keys:", list(result['overview'].keys()))
                else:
                    print("✗ 'overview' key missing")
            else:
                print("✗ Result is not a dictionary")
        except Exception as e:
            print("✗ Method execution failed:")
            print("Error:", str(e))
            traceback.print_exc()
    else:
        print("✗ Method does not exist")
        
except Exception as e:
    print("✗ Controller creation failed:")
    print("Error:", str(e))
    traceback.print_exc()
