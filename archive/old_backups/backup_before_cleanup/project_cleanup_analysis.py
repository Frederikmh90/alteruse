#!/usr/bin/env python3
"""
Project Cleanup Analysis Tool
============================
Systematically analyzes the alteruse project to identify:
1. File dependencies and import relationships
2. Usage patterns and active vs inactive scripts
3. Redundant files and version conflicts
4. Decision criteria for keep/delete decisions
"""

import os
import re
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict, Counter
import ast
import logging


class ProjectAnalyzer:
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.analysis_results = {
            "file_dependencies": {},
            "import_relationships": {},
            "usage_patterns": {},
            "redundant_files": [],
            "active_scripts": set(),
            "inactive_scripts": set(),
            "decision_criteria": {},
        }
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def analyze_project(self):
        """Main analysis method"""
        self.logger.info("Starting project analysis...")

        # Step 1: Find all Python files
        python_files = self.find_python_files()
        self.logger.info(f"Found {len(python_files)} Python files")

        # Step 2: Analyze imports and dependencies
        self.analyze_imports(python_files)

        # Step 3: Identify usage patterns
        self.identify_usage_patterns()

        # Step 4: Find redundant files
        self.find_redundant_files()

        # Step 5: Generate decision criteria
        self.generate_decision_criteria()

        # Step 6: Save analysis results
        self.save_analysis_results()

    def find_python_files(self) -> List[Path]:
        """Find all Python files in the project"""
        python_files = []
        for root, dirs, files in os.walk(self.project_root):
            # Skip virtual environment and hidden directories
            dirs[:] = [d for d in dirs if not d.startswith(".") and d != "venv"]

            for file in files:
                if file.endswith(".py"):
                    python_files.append(Path(root) / file)

        return python_files

    def analyze_imports(self, python_files: List[Path]):
        """Analyze import statements in Python files"""
        self.logger.info("Analyzing imports...")

        for file_path in python_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                # Extract imports using AST
                imports = self.extract_imports(content)

                # Store import information
                relative_path = file_path.relative_to(self.project_root)
                self.analysis_results["import_relationships"][str(relative_path)] = {
                    "imports": imports,
                    "import_count": len(imports),
                }

            except Exception as e:
                self.logger.warning(f"Error analyzing {file_path}: {e}")

    def extract_imports(self, content: str) -> List[str]:
        """Extract import statements from Python code"""
        imports = []
        try:
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    for alias in node.names:
                        if module:
                            imports.append(f"{module}.{alias.name}")
                        else:
                            imports.append(alias.name)
        except:
            # Fallback to regex for files that can't be parsed
            import_pattern = r"^(?:from\s+(\S+)\s+import\s+(\S+)|import\s+(\S+))"
            matches = re.findall(import_pattern, content, re.MULTILINE)
            for match in matches:
                if match[0] and match[1]:  # from ... import ...
                    imports.append(f"{match[0]}.{match[1]}")
                elif match[2]:  # import ...
                    imports.append(match[2])

        return imports

    def identify_usage_patterns(self):
        """Identify which scripts are actively used"""
        self.logger.info("Identifying usage patterns...")

        # Known active scripts (from user confirmation)
        known_active = {
            "notebooks/Processing_browser.py",
            "notebooks/Processing_facebook_batch_analysis.py",
            "notebooks/Processing_facebook_news_analysis.py",
        }

        # Scripts that are imported by others (indicating they're used)
        imported_scripts = set()
        for file_path, import_info in self.analysis_results[
            "import_relationships"
        ].items():
            for imported in import_info["imports"]:
                # Check if this import corresponds to a local file
                if imported.startswith("Processing_"):
                    imported_scripts.add(f"notebooks/{imported}.py")
                elif imported in [
                    "step1_extract_urls",
                    "step2_analyze_urls",
                    "step3_prioritize_domains",
                    "step4_scrape_content",
                ]:
                    imported_scripts.add(f"notebooks/url_extraction/{imported}.py")

        # Scripts that are referenced in shell scripts
        shell_referenced = self.find_shell_references()

        # Combine all active indicators
        self.analysis_results["active_scripts"] = (
            known_active | imported_scripts | shell_referenced
        )

        # Mark everything else as potentially inactive
        all_python_files = set(self.analysis_results["import_relationships"].keys())
        self.analysis_results["inactive_scripts"] = (
            all_python_files - self.analysis_results["active_scripts"]
        )

    def find_shell_references(self) -> Set[str]:
        """Find Python scripts referenced in shell scripts"""
        referenced = set()

        for root, dirs, files in os.walk(self.project_root):
            for file in files:
                if file.endswith(".sh"):
                    file_path = Path(root) / file
                    try:
                        with open(file_path, "r") as f:
                            content = f.read()

                        # Look for python3 commands
                        python_calls = re.findall(r"python3\s+([^\s]+\.py)", content)
                        for call in python_calls:
                            # Convert to relative path
                            if call.startswith("notebooks/"):
                                referenced.add(call)
                            elif call.startswith("./"):
                                referenced.add(call[2:])
                            else:
                                referenced.add(call)
                    except:
                        continue

        return referenced

    def find_redundant_files(self):
        """Identify redundant files (multiple versions, copies, etc.)"""
        self.logger.info("Finding redundant files...")

        # Group files by base name (without version suffixes)
        file_groups = defaultdict(list)

        for file_path in self.analysis_results["import_relationships"].keys():
            base_name = self.get_base_name(file_path)
            file_groups[base_name].append(file_path)

        # Find groups with multiple files
        for base_name, files in file_groups.items():
            if len(files) > 1:
                self.analysis_results["redundant_files"].append(
                    {"base_name": base_name, "files": files, "count": len(files)}
                )

    def get_base_name(self, file_path: str) -> str:
        """Extract base name without version suffixes"""
        # Remove common version patterns
        base = re.sub(r"_v\d+", "", file_path)
        base = re.sub(r"_copy", "", base)
        base = re.sub(r"_fixed", "", base)
        base = re.sub(r"_enhanced", "", base)
        base = re.sub(r"_optimized", "", base)
        base = re.sub(r"_resume_\d+", "", base)
        return base

    def generate_decision_criteria(self):
        """Generate decision criteria for keep/delete decisions"""
        self.logger.info("Generating decision criteria...")

        criteria = {
            "keep_criteria": {
                "known_active": "Files confirmed by user as actively used",
                "imported_by_others": "Files that are imported by other scripts",
                "referenced_in_shell": "Files referenced in shell scripts",
                "core_functionality": "Files providing core project functionality",
                "recent_modifications": "Files modified recently (last 30 days)",
            },
            "delete_criteria": {
                "older_versions": "Older versions of files with newer versions available",
                "unused_copies": "Copy files that are not referenced",
                "test_files": "Test files that are not part of main functionality",
                "debug_files": "Debug/temporary files",
                "duplicate_functionality": "Files with duplicate functionality",
            },
            "review_criteria": {
                "unclear_dependencies": "Files with unclear dependency relationships",
                "potential_utility": "Files that might be useful but not clearly used",
                "vm_integration": "Files related to VM-based processing",
            },
        }

        self.analysis_results["decision_criteria"] = criteria

    def save_analysis_results(self):
        """Save analysis results to files"""
        self.logger.info("Saving analysis results...")

        # Create output directory
        output_dir = Path("project_analysis")
        output_dir.mkdir(exist_ok=True)

        # Save detailed results
        with open(output_dir / "analysis_results.json", "w") as f:
            json.dump(self.analysis_results, f, indent=2, default=str)

        # Create summary report
        self.create_summary_report(output_dir)

        # Create decision matrix
        self.create_decision_matrix(output_dir)

    def create_summary_report(self, output_dir: Path):
        """Create a human-readable summary report"""
        report = []
        report.append("# Project Cleanup Analysis Summary")
        report.append("")

        # File counts
        total_files = len(self.analysis_results["import_relationships"])
        active_files = len(self.analysis_results["active_scripts"])
        inactive_files = len(self.analysis_results["inactive_scripts"])

        report.append(f"## File Statistics")
        report.append(f"- Total Python files: {total_files}")
        report.append(f"- Active files: {active_files}")
        report.append(f"- Potentially inactive: {inactive_files}")
        report.append("")

        # Active files
        report.append("## Active Files (Keep)")
        for file in sorted(self.analysis_results["active_scripts"]):
            report.append(f"- {file}")
        report.append("")

        # Potentially inactive files
        report.append("## Potentially Inactive Files (Review)")
        for file in sorted(self.analysis_results["inactive_scripts"]):
            report.append(f"- {file}")
        report.append("")

        # Redundant files
        report.append("## Redundant Files (Consider Consolidation)")
        for group in self.analysis_results["redundant_files"]:
            report.append(f"### {group['base_name']} ({group['count']} files)")
            for file in group["files"]:
                report.append(f"- {file}")
            report.append("")

        with open(output_dir / "summary_report.md", "w") as f:
            f.write("\n".join(report))

    def create_decision_matrix(self, output_dir: Path):
        """Create a decision matrix for systematic review"""
        matrix = []
        matrix.append("File,Status,Reason,Action")

        # Process active files
        for file in sorted(self.analysis_results["active_scripts"]):
            matrix.append(f"{file},Active,Known active script,KEEP")

        # Process potentially inactive files
        for file in sorted(self.analysis_results["inactive_scripts"]):
            matrix.append(f"{file},Review,No clear usage pattern,REVIEW")

        # Process redundant files
        for group in self.analysis_results["redundant_files"]:
            if group["count"] > 1:
                # Mark all but the most recent as candidates for deletion
                files = sorted(group["files"])
                for i, file in enumerate(files):
                    if i == 0:  # Keep the first one (usually the base version)
                        matrix.append(f"{file},Redundant,Multiple versions exist,KEEP")
                    else:
                        matrix.append(
                            f"{file},Redundant,Multiple versions exist,CONSIDER_DELETE"
                        )

        with open(output_dir / "decision_matrix.csv", "w") as f:
            f.write("\n".join(matrix))


if __name__ == "__main__":
    analyzer = ProjectAnalyzer()
    analyzer.analyze_project()
    print("Analysis complete! Check the 'project_analysis' directory for results.")
