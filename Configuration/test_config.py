"""
Test configuration file for CI/CD and local testing.
Uses environment variables when available (CI/CD), falls back to local values.
"""
import os

# CI/CD flag
IS_CI = os.getenv("CI", "false").lower() == "true"

# Oracle database
ORACLE_USER = os.getenv("ORACLE_USER", "system")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "sunbeam")
ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "FREEPDB1")

# MySQL database
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "sunbeam")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "retaildwh")

# PostgreSQL
POSTGRES_USER = os.getenv("POSTGRES_USER", "capstone")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "capstone")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "capstone2.cra4maemeqqs.ap-south-1.rds.amazonaws.com")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "capstone_src")

# Linux server (skip in CI)
hostname = os.getenv("LINUX_HOST", "192.168.56.103")
username = os.getenv("LINUX_USER", "root")
password = os.getenv("LINUX_PASSWORD", "root")
remote_file_path = "/root/sales_data.csv"
local_file_path = "SourceSystem/sales_data_linux.csv"
