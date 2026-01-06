#!/usr/bin/env python3
"""
Dependency Analysis Tool
========================
Analyzes dependencies of core processing scripts to ensure we don't delete critical files.
"""

import os
import re
import ast
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict


class DependencyAnalyzer:
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.core_scripts = [
            "notebooks/Processing_browser.py",
            "notebooks/Processing_facebook_batch_analysis.py",
            "notebooks/Processing_facebook_news_analysis.py",
        ]
        self.dependencies = defaultdict(set)
        self.local_imports = defaultdict(set)

    def analyze_core_dependencies(self):
        """Analyze dependencies of core processing scripts"""
        print("Analyzing dependencies of core processing scripts...")

        for script in self.core_scripts:
            if not (self.project_root / script).exists():
                print(f"Warning: Core script not found: {script}")
                continue

            print(f"\nAnalyzing: {script}")
            local_imports = self.extract_local_imports(script)

            if local_imports:
                print(f"  Local imports found:")
                for imp in local_imports:
                    print(f"    - {imp}")
                    self.local_imports[script].add(imp)
            else:
                print(f"  No local imports found")

    def extract_local_imports(self, script_path: str) -> List[str]:
        """Extract local imports from a Python script"""
        try:
            with open(self.project_root / script_path, "r", encoding="utf-8") as f:
                content = f.read()

            local_imports = []

            # Parse with AST
            try:
                tree = ast.parse(content)
                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom):
                        module = node.module
                        if module and not self.is_standard_library(module):
                            # Check if this is a local import
                            if self.is_local_module(module):
                                local_imports.append(module)
                    elif isinstance(node, ast.Import):
                        for alias in node.names:
                            if not self.is_standard_library(alias.name):
                                if self.is_local_module(alias.name):
                                    local_imports.append(alias.name)
            except:
                # Fallback to regex for files that can't be parsed
                import_pattern = r"from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+import"
                matches = re.findall(import_pattern, content)
                for match in matches:
                    if not self.is_standard_library(match) and self.is_local_module(
                        match
                    ):
                        local_imports.append(match)

            return list(set(local_imports))  # Remove duplicates

        except Exception as e:
            print(f"Error analyzing {script_path}: {e}")
            return []

    def is_standard_library(self, module: str) -> bool:
        """Check if a module is from the standard library"""
        stdlib_modules = {
            "os",
            "sys",
            "re",
            "json",
            "datetime",
            "pathlib",
            "typing",
            "collections",
            "urllib",
            "sqlite3",
            "pandas",
            "polars",
            "bs4",
            "requests",
            "numpy",
            "time",
            "random",
            "hashlib",
            "threading",
            "concurrent",
            "queue",
            "argparse",
            "signal",
            "traceback",
        }
        return module in stdlib_modules or module.startswith(
            ("os.", "sys.", "re.", "json.")
        )

    def is_local_module(self, module: str) -> bool:
        """Check if a module is local to the project"""
        # Check if the module file exists
        possible_paths = [
            self.project_root / f"{module}.py",
            self.project_root / "notebooks" / f"{module}.py",
            self.project_root / "notebooks" / "url_extraction" / f"{module}.py",
            self.project_root
            / "notebooks"
            / "url_extraction_facebook"
            / f"{module}.py",
        ]

        return any(path.exists() for path in possible_paths)

    def find_critical_dependencies(self) -> Set[str]:
        """Find all critical dependencies that should not be deleted"""
        critical_files = set()

        # Add core scripts themselves
        critical_files.update(self.core_scripts)

        # Add all local imports
        for script, imports in self.local_imports.items():
            for imp in imports:
                # Find the actual file path
                file_path = self.find_import_file_path(imp)
                if file_path:
                    critical_files.add(file_path)

        return critical_files

    def find_import_file_path(self, module: str) -> str:
        """Find the file path for a local import"""
        possible_paths = [
            f"{module}.py",
            f"notebooks/{module}.py",
            f"notebooks/url_extraction/{module}.py",
            f"notebooks/url_extraction_facebook/{module}.py",
        ]

        for path in possible_paths:
            if (self.project_root / path).exists():
                return path

        return None

    def generate_dependency_report(self):
        """Generate a comprehensive dependency report"""
        print("\n" + "=" * 60)
        print("DEPENDENCY ANALYSIS REPORT")
        print("=" * 60)

        # Find critical dependencies
        critical_files = self.find_critical_dependencies()

        print(f"\nCore Processing Scripts ({len(self.core_scripts)}):")
        for script in self.core_scripts:
            print(f"  - {script}")

        print(f"\nCritical Dependencies ({len(critical_files)}):")
        for file in sorted(critical_files):
            print(f"  - {file}")

        print(f"\nLocal Import Relationships:")
        for script, imports in self.local_imports.items():
            if imports:
                print(f"  {script}:")
                for imp in imports:
                    file_path = self.find_import_file_path(imp)
                    print(f"    -> {imp} ({file_path})")

        # Save report
        self.save_dependency_report(critical_files)

        return critical_files

    def save_dependency_report(self, critical_files: Set[str]):
        """Save dependency report to file"""
        report = []
        report.append("# Critical Dependencies Report")
        report.append("")
        report.append("## Core Processing Scripts")
        for script in self.core_scripts:
            report.append(f"- {script}")
        report.append("")

        report.append("## Critical Dependencies (DO NOT DELETE)")
        for file in sorted(critical_files):
            report.append(f"- {file}")
        report.append("")

        report.append("## Import Relationships")
        for script, imports in self.local_imports.items():
            if imports:
                report.append(f"### {script}")
                for imp in imports:
                    file_path = self.find_import_file_path(imp)
                    report.append(f"- imports: {imp} ({file_path})")
                report.append("")

        with open("critical_dependencies_report.md", "w") as f:
            f.write("\n".join(report))

        print(f"\nDependency report saved to: critical_dependencies_report.md")


def main():
    analyzer = DependencyAnalyzer()
    analyzer.analyze_core_dependencies()
    critical_files = analyzer.generate_dependency_report()

    print(f"\nTotal critical files identified: {len(critical_files)}")
    print(
        "These files should NOT be deleted as they are dependencies of core processing scripts."
    )


if __name__ == "__main__":
    main()
