"""
Basic sanity tests to verify setup is correct
"""
import pytest
import os
import sys

@pytest.mark.smoke
@pytest.mark.unit
def test_project_structure():
    """Test that basic project structure exists"""
    assert os.path.exists("Configuration"), "Configuration directory missing"
    assert os.path.exists("TestData"), "TestData directory missing"
    assert os.path.exists("TestScripts"), "TestScripts directory missing"
    print("✅ Project structure is correct")

@pytest.mark.smoke
@pytest.mark.unit
def test_python_version():
    """Test Python version"""
    assert sys.version_info >= (3, 8), "Python 3.8+ required"
    print(f"✅ Python version: {sys.version}")

@pytest.mark.smoke
@pytest.mark.unit
def test_imports():
    """Test that key modules can be imported"""
    try:
        from Configuration import test_config
        from TestUtilities import utilities
        print("✅ All imports successful")
    except ImportError as e:
        pytest.fail(f"Import failed: {e}")

@pytest.mark.smoke
@pytest.mark.unit  
def test_test_data_files_exist():
    """Test that test data files exist"""
    test_files = [
        "TestData/sales_data_linux.csv",
        "TestData/product_data.csv",
        "TestData/inventory_data.xml",
        "TestData/supplier_data.json"
    ]
    
    for file in test_files:
        assert os.path.exists(file), f"Test data file missing: {file}"
    
    print("✅ All test data files exist")
