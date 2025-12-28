"""
Pytest configuration and fixtures for ETL testing.
Works both locally and in CI/CD environments.
"""
import os
import sys
import pytest
from sqlalchemy import create_engine
import logging

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Configuration.test_config import *

# Logging configuration
logging.basicConfig(
    filename="Logs/etljob.log",
    filemode="w",
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def connect_to_mysql_database():
    """Fixture to connect to MySQL database (works in CI and locally)"""
    logger.info("MySQL database connection is being established...")
    
    try:
        connection_string = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
        logger.info(f"Connecting to MySQL: {MYSQL_USER}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
        
        mysql_engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False
        )
        
        # Test connection
        with mysql_engine.connect() as conn:
            logger.info("MySQL connection test successful")
        
        logger.info("MySQL database connection established")
        yield mysql_engine
        
    except Exception as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        pytest.skip(f"MySQL not available: {e}")
        yield None
    finally:
        if 'mysql_engine' in locals():
            mysql_engine.dispose()
            logger.info("MySQL connection closed")


@pytest.fixture(scope="session")
def connect_to_oracle_database():
    """Fixture to connect to Oracle database (skip in CI if not available)"""
    logger.info("Oracle database connection is being established...")
    
    # Skip Oracle tests in CI unless explicitly configured
    if IS_CI and not os.getenv("RUN_ORACLE_TESTS"):
        pytest.skip("Oracle tests skipped in CI environment")
    
    try:
        connection_string = f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/?service_name={ORACLE_SERVICE}"
        logger.info(f"Connecting to Oracle: {ORACLE_USER}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
        
        oracle_engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        # Test connection
        with oracle_engine.connect() as conn:
            logger.info("Oracle connection test successful")
        
        logger.info("Oracle database connection established")
        yield oracle_engine
        
    except Exception as e:
        logger.error(f"Failed to connect to Oracle: {e}")
        pytest.skip(f"Oracle not available: {e}")
        yield None
    finally:
        if 'oracle_engine' in locals():
            oracle_engine.dispose()
            logger.info("Oracle connection closed")


def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line("markers", "smoke: smoke tests")
    config.addinivalue_line("markers", "regression: regression tests")
    config.addinivalue_line("markers", "dq: data quality tests")
    config.addinivalue_line("markers", "integration: integration tests")
    config.addinivalue_line("markers", "unit: unit tests")
