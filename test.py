#!/usr/bin/env python3
"""
V3 COMPLETE TESTING - DETECTION + GUIDED FIXES + IMPORT TESTING
===============================================================
Phase 1: Detect mock data violations (safe scanning)
Phase 2: Provide fix recommendations 
Phase 3: Test imports after manual fixes (optional)

NO AUTOMATIC CODE MODIFICATION - Only detection and guidance
"""

import os
import ast
import sys
import re
import importlib.util
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import requests
from contextlib import redirect_stdout, redirect_stderr
import io

class MockDataDetector:
    """Detects mock data patterns and provides fix guidance"""
    
    def __init__(self):
        self.violation_patterns = {
            # Critical violations (must fix)
            'critical': [
                (r'MockClient\(', 'Replace with real Binance Client'),
                (r'FakeClient\(', 'Replace with real exchange client'),
                (r'generate_mock_', 'Remove mock data generation'),
                (r'create_fake_', 'Remove fake data creation'),
                (r'mock_data\s*=\s*True', 'Set to False or remove'),
                (r'use_mock\s*=\s*True', 'Set to False'),
            ],
            
            # High priority violations  
            'high': [
                (r'if.*not.*available.*mock', 'Remove mock fallback, handle error properly'),
                (r'except.*mock', 'Remove mock exception handling'),
                (r'creating mock', 'Remove mock creation logic'),
                (r'TestClient\(', 'Replace with real client'),
                (r'simulate_data\(', 'Use real data source'),
            ],
            
            # Medium priority (review needed)
            'medium': [
                (r'random\.(uniform|choice|randint)', 'Ensure this is not generating fake prices'),
                (r'numpy\.random', 'Ensure this is not generating fake market data'),
                (r'test_mode\s*=\s*True', 'Review if this enables mock data'),
            ]
        }
        
        self.fix_templates = {
            'MockClient': '''
# BEFORE:
if not client:
    client = MockClient()

# AFTER:
if not client:
    api_key = os.getenv('BINANCE_API_KEY_1')
    api_secret = os.getenv('BINANCE_API_SECRET_1')
    if not api_key or not api_secret:
        raise ValueError("Missing Binance API credentials")
    client = Client(api_key, api_secret, testnet=True)
''',
            
            'generate_mock': '''
# BEFORE:
def generate_mock_data():
    return fake_prices

# AFTER:
def get_real_market_data(symbol, interval):
    return client.get_historical_klines(symbol, interval, "100")
''',
            
            'fallback_pattern': '''
# BEFORE:
try:
    data = fetch_real_data()
except:
    data = create_mock_data()

# AFTER:
try:
    data = fetch_real_data()
except Exception as e:
    logger.error(f"Failed to fetch real data: {e}")
    raise  # Don't fallback to mock - fail fast
'''
        }

class V3SystemTester:
    """Complete V3 system testing with guided fixes"""
    
    def __init__(self):
        self.detector = MockDataDetector()
        self.scan_results = {}
        self.import_results = {}
        
    def phase1_detect_violations(self) -> Dict:
        """Phase 1: Detect violations without importing"""
        print("=" * 60)
        print("PHASE 1: SCANNING FOR MOCK DATA VIOLATIONS")
        print("=" * 60)
        
        python_files = list(Path('.').rglob('*.py'))
        python_files = [f for f in python_files if not any(skip in str(f) for skip in ['.git', '__pycache__', 'venv', 'env'])]
        
        total_violations = 0
        files_with_violations = []
        all_violations = []
        
        for file_path in sorted(python_files):
            if file_path.name in ['complete_test.py', 'test.py', 'test_fixed.py']:
                continue
                
            violations = self._scan_file(file_path)
            
            if violations:
                files_with_violations.append(str(file_path))
                total_violations += len(violations)
                all_violations.extend(violations)
                
                print(f"\nVIOLATIONS FOUND: {file_path}")
                for violation in violations:
                    severity_icon = {"critical": "??", "high": "??", "medium": "??"}[violation['severity']]
                    print(f"   {severity_icon} Line {violation['line']}: {violation['content'][:60]}...")
                    print(f"      Fix: {violation['fix_suggestion']}")
            
        print(f"\n" + "=" * 60)
        print("PHASE 1 RESULTS:")
        print(f"Files scanned: {len(python_files)}")
        print(f"Files with violations: {len(files_with_violations)}")
        print(f"Total violations: {total_violations}")
        
        return {
            'total_files': len(python_files),
            'files_with_violations': files_with_violations,
            'total_violations': total_violations,
            'violations': all_violations
        }
    
    def _scan_file(self, file_path: Path) -> List[Dict]:
        """Scan individual file for violations"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            violations = []
            
            for i, line in enumerate(lines, 1):
                line_clean = line.strip()
                if line_clean.startswith('#'):
                    continue
                    
                for severity, patterns in self.detector.violation_patterns.items():
                    for pattern, fix_suggestion in patterns:
                        if re.search(pattern, line, re.IGNORECASE):
                            violations.append({
                                'file': str(file_path),
                                'line': i,
                                'content': line_clean,
                                'pattern': pattern,
                                'severity': severity,
                                'fix_suggestion': fix_suggestion
                            })
                            break
            
            return violations
            
        except Exception as e:
            return []
    
    def phase2_generate_fix_guide(self, scan_results: Dict):
        """Phase 2: Generate detailed fix guide"""
        print("\n" + "=" * 60)
        print("PHASE 2: GUIDED FIXES - MANUAL IMPLEMENTATION REQUIRED")
        print("=" * 60)
        
        if scan_results['total_violations'] == 0:
            print("? No violations found - system appears clean!")
            return
        
        print("?? CRITICAL FIXES NEEDED:")
        print("The following files need manual fixes:\n")
        
        # Group violations by file
        violations_by_file = {}
        for violation in scan_results['violations']:
            file_path = violation['file']
            if file_path not in violations_by_file:
                violations_by_file[file_path] = []
            violations_by_file[file_path].append(violation)
        
        for file_path, violations in violations_by_file.items():
            print(f"?? {file_path}")
            critical_count = sum(1 for v in violations if v['severity'] == 'critical')
            high_count = sum(1 for v in violations if v['severity'] == 'high')
            
            print(f"   Critical: {critical_count}, High: {high_count}")
            
            for violation in violations[:3]:  # Show first 3
                print(f"   Line {violation['line']}: {violation['fix_suggestion']}")
            
            if len(violations) > 3:
                print(f"   ... and {len(violations) - 3} more violations")
            print()
        
        # Show fix examples
        print("?? EXAMPLE FIXES:")
        print(self.detector.fix_templates['MockClient'])
        print(self.detector.fix_templates['generate_mock'])
        
        print("?? IMPORTANT: Fix these manually, don't let any auto-tools modify your trading code!")
    
    def phase3_test_imports(self, force: bool = False) -> Dict:
        """Phase 3: Test imports after manual fixes (optional)"""
        if not force:
            response = input("\nRun Phase 3 import testing? This will try to import modules. (y/n): ")
            if response.lower() != 'y':
                return {'skipped': True}
        
        print("\n" + "=" * 60)
        print("PHASE 3: TESTING IMPORTS AFTER FIXES")
        print("=" * 60)
        print("?? This will attempt to import modules - stop if mock creation warnings appear")
        
        python_files = [f for f in Path('.').rglob('*.py') if f.suffix == '.py']
        python_files = [f for f in python_files if not any(skip in str(f) for skip in ['.git', '__pycache__', 'venv'])]
        
        import_results = {
            'total_files': len(python_files),
            'successful_imports': 0,
            'failed_imports': 0,
            'mock_warnings': 0,
            'results': []
        }
        
        for file_path in sorted(python_files):
            if file_path.name in ['complete_test.py', 'test.py', 'test_fixed.py']:
                continue
                
            result = self._test_import(file_path)
            import_results['results'].append(result)
            
            if result['success']:
                import_results['successful_imports'] += 1
                print(f"? {file_path.name}")
            else:
                import_results['failed_imports'] += 1
                print(f"? {file_path.name}: {result['error']}")
                
            if result['has_mock_warnings']:
                import_results['mock_warnings'] += 1
                print(f"   ?? Mock warnings detected - check this file")
        
        print(f"\nIMPORT TEST RESULTS:")
        print(f"Successful: {import_results['successful_imports']}")
        print(f"Failed: {import_results['failed_imports']}")
        print(f"Mock warnings: {import_results['mock_warnings']}")
        
        return import_results
    
    def _test_import(self, file_path: Path) -> Dict:
        """Test importing a single file"""
        try:
            # Capture output
            stdout_capture = io.StringIO()
            stderr_capture = io.StringIO()
            
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                spec = importlib.util.spec_from_file_location(file_path.stem, file_path)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
            
            # Check for mock warnings
            output = stdout_capture.getvalue() + stderr_capture.getvalue()
            has_mock_warnings = any(word in output.lower() for word in ['mock', 'fake', 'creating mock', 'fallback'])
            
            return {
                'file': str(file_path),
                'success': True,
                'error': None,
                'has_mock_warnings': has_mock_warnings,
                'output': output
            }
            
        except Exception as e:
            return {
                'file': str(file_path),
                'success': False,
                'error': str(e),
                'has_mock_warnings': False,
                'output': ''
            }
    
    def check_flask_status(self):
        """Check Flask server status"""
        try:
            port = int(os.getenv('FLASK_PORT', '8102'))
            response = requests.get(f"http://127.0.0.1:{port}/health", timeout=3)
            return True, f"Flask responding on port {port}"
        except:
            port = int(os.getenv('FLASK_PORT', '8102'))
            return False, f"Flask not responding on port {port}"
    
    def run_complete_test(self):
        """Run complete testing process"""
        print("?? V3 COMPLETE TESTING - DETECTION + GUIDED FIXES + IMPORT TESTING")
        print("Real Data Only System - No Mock Data Allowed")
        print()
        
        # Check Flask first
        flask_ok, flask_msg = self.check_flask_status()
        print(f"Flask Status: {'?' if flask_ok else '?'} {flask_msg}")
        
        # Phase 1: Detection
        scan_results = self.phase1_detect_violations()
        
        # Phase 2: Fix guidance
        self.phase2_generate_fix_guide(scan_results)
        
        # Phase 3: Import testing (optional)
        if scan_results['total_violations'] == 0:
            print("? System clean - ready for import testing")
            import_results = self.phase3_test_imports()
        else:
            print("\n?? FIX VIOLATIONS FIRST before import testing")
            print("Run this test again after manual fixes")
            import_results = {'skipped_due_to_violations': True}
        
        # Final report
        self._generate_final_report(scan_results, import_results, flask_ok)
    
    def _generate_final_report(self, scan_results: Dict, import_results: Dict, flask_ok: bool):
        """Generate final report"""
        print("\n" + "=" * 60)
        print("FINAL REPORT")
        print("=" * 60)
        
        total_issues = scan_results['total_violations']
        mock_warnings = import_results.get('mock_warnings', 0)
        
        print(f"Scan Results:")
        print(f"   Violations found: {scan_results['total_violations']}")
        print(f"   Files needing fixes: {len(scan_results['files_with_violations'])}")
        
        if not import_results.get('skipped') and not import_results.get('skipped_due_to_violations'):
            print(f"Import Results:")
            print(f"   Successful imports: {import_results.get('successful_imports', 0)}")
            print(f"   Mock warnings: {mock_warnings}")
        
        print(f"\nSYSTEM STATUS:")
        if total_issues == 0 and mock_warnings == 0 and flask_ok:
            print("   ? SYSTEM READY - No violations found")
            print("   ? Safe to start: python3 main.py")
        else:
            print("   ?? FIXES REQUIRED:")
            if total_issues > 0:
                print(f"      - Fix {total_issues} mock data violations")
            if mock_warnings > 0:
                print(f"      - Investigate {mock_warnings} mock warnings")
            if not flask_ok:
                print(f"      - Fix Flask connectivity")
        
        print(f"\nREAL DATA ONLY SYSTEM:")
        print(f"   Paper Trading: Real data + simulated orders")
        print(f"   Live Trading: Real data + real orders")
        print(f"   Backtesting: Real historical data only")

def main():
    """Main testing function"""
    tester = V3SystemTester()
    tester.run_complete_test()

if __name__ == "__main__":
    main()