#!/usr/bin/env python3
"""
Script to automatically fix import statements in test files
after migrating to unified project structure.
"""

import os
import re
import sys


def fix_imports_in_file(filepath):
    """Fix import statements in a single file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        original_content = content

        # Fix imports
        replacements = [
            # Fix utilities import
            (r'from CommonUtilities\.utilities import', 'from TestUtilities.utilities import'),
            (r'from CommonUtilities import utilities', 'from TestUtilities import utilities'),

            # Fix config import
            (r'from Configuration\.etlconfig import', 'from Configuration.test_config import'),
            (r'from Configuration import etlconfig', 'from Configuration import test_config'),
        ]

        for old_pattern, new_pattern in replacements:
            content = re.sub(old_pattern, new_pattern, content)

        # Only write if changes were made
        if content != original_content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            return True, "Updated"
        else:
            return False, "No changes needed"

    except Exception as e:
        return False, f"Error: {str(e)}"


def main():
    """Main function to process all test files"""
    print("=" * 60)
    print("Fixing Import Statements in Test Files")
    print("=" * 60)
    print()

    # Find all Python files in TestScripts directory
    test_dir = "TestScripts"

    if not os.path.exists(test_dir):
        print(f"❌ Error: {test_dir} directory not found!")
        print("Make sure you're running this from the project root.")
        sys.exit(1)

    python_files = []
    for root, dirs, files in os.walk(test_dir):
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))

    if not python_files:
        print(f"No Python files found in {test_dir}")
        return

    print(f"Found {len(python_files)} Python files to check:")
    print()

    updated_count = 0
    for filepath in python_files:
        print(f"Processing: {filepath}...", end=" ")
        changed, status = fix_imports_in_file(filepath)
        print(status)
        if changed:
            updated_count += 1

    print()
    print("=" * 60)
    print(f"✅ Complete! Updated {updated_count}/{len(python_files)} files")
    print("=" * 60)


if __name__ == "__main__":
    main()