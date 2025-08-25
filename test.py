#!/usr/bin/env python3
"""
Comprehensive Python Test Harness
Automatically discovers and tests all Python files in a directory.
Tests imports, syntax, dependencies, and runtime safety.
"""

import os
import sys
import importlib.util
import subprocess
import tempfile
import shutil
import ast
import traceback
import time
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass, asdict
from contextlib import contextmanager, redirect_stdout, redirect_stderr
import io

import pytest


@dataclass
class TestResult:
    """Data class to store test results for each file."""
    file_path: str
    file_name: str
    import_success: bool
    import_error: Optional[str]
    syntax_valid: bool
    syntax_error: Optional[str]
    dependencies_met: bool
    missing_dependencies: List[str]
    runtime_safe: bool
    runtime_error: Optional[str]
    execution_time: float
    file_size: int
    lines_of_code: int
    test_timestamp: str
    warnings: List[str]


class PythonTestHarness:
    """Comprehensive test harness for Python files."""
    
    def __init__(self, directory: str = ".", exclude_patterns: List[str] = None):
        self.directory = Path(directory).resolve()
        self.exclude_patterns = exclude_patterns or ["__pycache__", ".git", ".pytest_cache", "venv", "env"]
        self.results: Dict[str, TestResult] = {}
        self.test_start_time = time.time()
        
    def discover_python_files(self) -> List[Path]:
        """Discover all Python files in the directory."""
        python_files = []
        
        for root, dirs, files in os.walk(self.directory):
            # Exclude certain directories
            dirs[:] = [d for d in dirs if not any(pattern in d for pattern in self.exclude_patterns)]
            
            for file in files:
                if file.endswith('.py') and not file.startswith('test_'):
                    file_path = Path(root) / file
                    # Skip this test file itself
                    if file_path.name != Path(__file__).name:
                        python_files.append(file_path)
        
        return sorted(python_files)
    
    def check_syntax(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """Check if the Python file has valid syntax."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            ast.parse(content)
            return True, None
            
        except SyntaxError as e:
            return False, f"Line {e.lineno}: {e.msg}"
        except Exception as e:
            return False, f"Parse error: {str(e)}"
    
    def extract_imports(self, file_path: Path) -> List[str]:
        """Extract all import statements from a Python file."""
        imports = []
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(alias.name.split('.')[0])
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        imports.append(node.module.split('.')[0])
                        
        except Exception:
            pass
            
        return list(set(imports))  # Remove duplicates
    
    def check_dependencies(self, imports: List[str]) -> Tuple[bool, List[str]]:
        """Check if all required dependencies are available."""
        missing = []
        
        # Standard library modules (common ones)
        stdlib_modules = {
            'os', 'sys', 'time', 'datetime', 'json', 'urllib', 'http',
            're', 'math', 'random', 'itertools', 'collections', 'functools',
            'pathlib', 'subprocess', 'threading', 'multiprocessing', 'asyncio',
            'logging', 'argparse', 'configparser', 'sqlite3', 'csv', 'xml',
            'html', 'email', 'base64', 'hashlib', 'hmac', 'uuid', 'tempfile',
            'shutil', 'glob', 'fnmatch', 'pickle', 'copy', 'operator',
            'typing', 'dataclasses', 'contextlib', 'warnings', 'traceback',
            'io', 'socket', 'ssl', 'zipfile', 'tarfile', 'gzip'
        }
        
        for module_name in imports:
            if module_name in stdlib_modules:
                continue
                
            try:
                importlib.import_module(module_name)
            except ImportError:
                missing.append(module_name)
            except Exception:
                # Module exists but might have issues
                pass
                
        return len(missing) == 0, missing
    
    @contextmanager
    def sandboxed_environment(self):
        """Create a sandboxed environment for testing."""
        # Save current state
        old_path = sys.path.copy()
        old_modules = sys.modules.copy()
        old_argv = sys.argv.copy()
        
        # Create temporary directory for any file operations
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Modify environment
                sys.argv = ['test_script']
                
                yield temp_dir
                
            finally:
                # Restore state
                sys.path[:] = old_path
                # Only restore modules that were present before
                modules_to_remove = set(sys.modules.keys()) - set(old_modules.keys())
                for module in modules_to_remove:
                    try:
                        del sys.modules[module]
                    except KeyError:
                        pass
                sys.argv[:] = old_argv
    
    def safe_import_test(self, file_path: Path) -> Tuple[bool, Optional[str], List[str]]:
        """Safely test importing a Python file."""
        warnings_list = []
        
        try:
            with self.sandboxed_environment():
                # Capture stdout and stderr
                stdout_capture = io.StringIO()
                stderr_capture = io.StringIO()
                
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    # Create module spec
                    spec = importlib.util.spec_from_file_location(
                        file_path.stem, file_path
                    )
                    
                    if spec is None:
                        return False, "Could not create module spec", warnings_list
                    
                    module = importlib.util.module_from_spec(spec)
                    
                    # Add to sys.modules temporarily
                    sys.modules[file_path.stem] = module
                    
                    # Execute the module
                    spec.loader.exec_module(module)
                
                # Check for warnings in stderr
                stderr_content = stderr_capture.getvalue()
                if stderr_content:
                    warnings_list.extend(stderr_content.strip().split('\n'))
                
                return True, None, warnings_list
                
        except ImportError as e:
            return False, f"Import error: {str(e)}", warnings_list
        except ModuleNotFoundError as e:
            return False, f"Module not found: {str(e)}", warnings_list
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            return False, error_msg, warnings_list
    
    def runtime_safety_test(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """Test if the file can be executed safely without running main functions."""
        try:
            # Read the file content
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Check for dangerous patterns
            dangerous_patterns = [
                'os.system(',
                'subprocess.call(',
                'eval(',
                'exec(',
                '__import__(',
                'open(',  # File operations outside of main
                'shutil.rmtree(',
                'os.remove(',
                'os.rmdir('
            ]
            
            # Check if code is wrapped in if __name__ == "__main__"
            has_main_guard = 'if __name__ ==' in content
            
            # Look for dangerous patterns outside of main guard
            if not has_main_guard:
                lines = content.split('\n')
                for i, line in enumerate(lines, 1):
                    for pattern in dangerous_patterns:
                        if pattern in line and not line.strip().startswith('#'):
                            return False, f"Potentially unsafe operation on line {i}: {pattern}"
            
            return True, None
            
        except Exception as e:
            return False, f"Safety check failed: {str(e)}"
    
    def get_file_stats(self, file_path: Path) -> Tuple[int, int]:
        """Get file size and lines of code."""
        try:
            file_size = file_path.stat().st_size
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            # Count non-empty, non-comment lines
            loc = 0
            for line in lines:
                stripped = line.strip()
                if stripped and not stripped.startswith('#'):
                    loc += 1
                    
            return file_size, loc
            
        except Exception:
            return 0, 0
    
    def test_single_file(self, file_path: Path) -> TestResult:
        """Test a single Python file comprehensively."""
        start_time = time.time()
        
        # Basic file info
        file_size, lines_of_code = self.get_file_stats(file_path)
        
        # Test syntax
        syntax_valid, syntax_error = self.check_syntax(file_path)
        
        # Extract and check dependencies
        imports = self.extract_imports(file_path)
        dependencies_met, missing_deps = self.check_dependencies(imports)
        
        # Test import safety
        import_success, import_error, warnings = self.safe_import_test(file_path)
        
        # Test runtime safety
        runtime_safe, runtime_error = self.runtime_safety_test(file_path)
        
        execution_time = time.time() - start_time
        
        return TestResult(
            file_path=str(file_path),
            file_name=file_path.name,
            import_success=import_success,
            import_error=import_error,
            syntax_valid=syntax_valid,
            syntax_error=syntax_error,
            dependencies_met=dependencies_met,
            missing_dependencies=missing_deps,
            runtime_safe=runtime_safe,
            runtime_error=runtime_error,
            execution_time=execution_time,
            file_size=file_size,
            lines_of_code=lines_of_code,
            test_timestamp=time.strftime('%Y-%m-%d %H:%M:%S'),
            warnings=warnings
        )
    
    def run_all_tests(self) -> Dict[str, TestResult]:
        """Run tests on all discovered Python files."""
        python_files = self.discover_python_files()
        
        print(f"\n🔍 Discovered {len(python_files)} Python files to test")
        print(f"📁 Testing directory: {self.directory}")
        print("=" * 80)
        
        for file_path in python_files:
            print(f"🧪 Testing: {file_path.name}")
            result = self.test_single_file(file_path)
            self.results[str(file_path)] = result
            
            # Quick status indicator
            status_symbols = []
            status_symbols.append("✅" if result.syntax_valid else "❌")
            status_symbols.append("📦" if result.dependencies_met else "⚠️")
            status_symbols.append("🔒" if result.runtime_safe else "⚠️")
            status_symbols.append("✅" if result.import_success else "❌")
            
            print(f"   {''.join(status_symbols)} ({result.execution_time:.3f}s)")
        
        return self.results
    
    def generate_report(self) -> str:
        """Generate a comprehensive test report."""
        if not self.results:
            return "No test results available."
        
        total_files = len(self.results)
        passed = sum(1 for r in self.results.values() 
                    if r.syntax_valid and r.dependencies_met and r.runtime_safe and r.import_success)
        failed = total_files - passed
        
        total_time = time.time() - self.test_start_time
        total_loc = sum(r.lines_of_code for r in self.results.values())
        total_size = sum(r.file_size for r in self.results.values())
        
        report = [
            "\n" + "=" * 80,
            "🧪 PYTHON TEST HARNESS REPORT",
            "=" * 80,
            f"📊 Summary: {passed} passed, {failed} failed, {total_files} total",
            f"⏱️  Total execution time: {total_time:.2f}s",
            f"📏 Total lines of code: {total_loc:,}",
            f"💾 Total file size: {total_size:,} bytes",
            f"📅 Test completed: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "📋 DETAILED RESULTS:",
            "-" * 80
        ]
        
        # Sort results by status (failed first, then passed)
        sorted_results = sorted(
            self.results.values(),
            key=lambda x: (x.import_success and x.syntax_valid and x.dependencies_met and x.runtime_safe)
        )
        
        for result in sorted_results:
            overall_status = (result.import_success and result.syntax_valid and 
                            result.dependencies_met and result.runtime_safe)
            
            status_icon = "✅ PASS" if overall_status else "❌ FAIL"
            
            report.extend([
                f"\n{status_icon} {result.file_name}",
                f"   📁 Path: {result.file_path}",
                f"   📏 Size: {result.file_size:,} bytes, {result.lines_of_code} LOC",
                f"   ⏱️  Test time: {result.execution_time:.3f}s"
            ])
            
            # Detailed status
            if not result.syntax_valid:
                report.append(f"   ❌ Syntax Error: {result.syntax_error}")
            
            if not result.dependencies_met:
                deps = ", ".join(result.missing_dependencies)
                report.append(f"   ⚠️  Missing Dependencies: {deps}")
            
            if not result.runtime_safe:
                report.append(f"   ⚠️  Runtime Safety: {result.runtime_error}")
            
            if not result.import_success:
                report.append(f"   ❌ Import Error: {result.import_error}")
            
            if result.warnings:
                for warning in result.warnings:
                    if warning.strip():
                        report.append(f"   ⚠️  Warning: {warning.strip()}")
        
        return "\n".join(report)
    
    def save_json_report(self, filename: str = "test_results.json"):
        """Save test results to a JSON file."""
        results_dict = {path: asdict(result) for path, result in self.results.items()}
        
        with open(filename, 'w') as f:
            json.dump({
                'test_summary': {
                    'total_files': len(self.results),
                    'passed': sum(1 for r in self.results.values() 
                                if r.syntax_valid and r.dependencies_met and r.runtime_safe and r.import_success),
                    'test_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'total_execution_time': time.time() - self.test_start_time
                },
                'results': results_dict
            }, f, indent=2)
        
        print(f"💾 JSON report saved to: {filename}")


# Pytest integration
class TestPythonFiles:
    """Pytest test class for discovered Python files."""
    
    @classmethod
    def setup_class(cls):
        """Setup test class with harness."""
        cls.harness = PythonTestHarness()
        cls.harness.run_all_tests()
    
    def test_file_syntax(self):
        """Test that all files have valid syntax."""
        failed_files = []
        for path, result in self.harness.results.items():
            if not result.syntax_valid:
                failed_files.append(f"{result.file_name}: {result.syntax_error}")
        
        assert not failed_files, f"Syntax errors in files:\n" + "\n".join(failed_files)
    
    def test_file_imports(self):
        """Test that all files can be imported successfully."""
        failed_files = []
        for path, result in self.harness.results.items():
            if not result.import_success:
                failed_files.append(f"{result.file_name}: {result.import_error}")
        
        assert not failed_files, f"Import errors in files:\n" + "\n".join(failed_files)
    
    def test_dependencies(self):
        """Test that all required dependencies are available."""
        failed_files = []
        for path, result in self.harness.results.items():
            if not result.dependencies_met:
                deps = ", ".join(result.missing_dependencies)
                failed_files.append(f"{result.file_name}: Missing {deps}")
        
        assert not failed_files, f"Missing dependencies:\n" + "\n".join(failed_files)
    
    def test_runtime_safety(self):
        """Test that all files are safe to import."""
        failed_files = []
        for path, result in self.harness.results.items():
            if not result.runtime_safe:
                failed_files.append(f"{result.file_name}: {result.runtime_error}")
        
        assert not failed_files, f"Runtime safety issues:\n" + "\n".join(failed_files)


def main():
    """Main function to run the test harness."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Comprehensive Python Test Harness")
    parser.add_argument(
        "directory", 
        nargs="?", 
        default=".", 
        help="Directory to test (default: current directory)"
    )
    parser.add_argument(
        "--json-report", 
        action="store_true", 
        help="Generate JSON report"
    )
    parser.add_argument(
        "--pytest", 
        action="store_true", 
        help="Run with pytest integration"
    )
    parser.add_argument(
        "--exclude", 
        nargs="*", 
        default=["__pycache__", ".git", ".pytest_cache", "venv", "env"],
        help="Patterns to exclude from testing"
    )
    
    args = parser.parse_args()
    
    if args.pytest:
        # Run with pytest
        pytest.main([__file__, "-v", "--tb=short"])
    else:
        # Run standalone
        harness = PythonTestHarness(args.directory, args.exclude)
        harness.run_all_tests()
        
        # Generate and print report
        report = harness.generate_report()
        print(report)
        
        # Save JSON report if requested
        if args.json_report:
            harness.save_json_report()
        
        # Exit with error code if any tests failed
        failed_count = sum(1 for r in harness.results.values() 
                          if not (r.syntax_valid and r.dependencies_met and r.runtime_safe and r.import_success))
        
        if failed_count > 0:
            print(f"\n❌ {failed_count} file(s) failed testing")
            sys.exit(1)
        else:
            print(f"\n✅ All {len(harness.results)} files passed testing")


if __name__ == "__main__":
    main()