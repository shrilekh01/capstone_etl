# ETL Pipeline with Automated Testing

This project implements an ETL (Extract, Transform, Load) pipeline with comprehensive automated testing using pytest and GitHub Actions.

## Project Structure
```
etl-project/
├── CoreScripts/          # ETL pipeline code
├── TestScripts/          # Test suite
├── Configuration/        # Configuration files
├── TestData/            # Test data files
├── SourceSystem/        # Source data files
└── .github/workflows/   # CI/CD workflows
```

## Setup

### Local Development

1. Clone the repository:
```bash
git clone <your-repo-url>
cd etl-project
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
pip install -r requirements-test.txt
```

4. Configure databases:
   - Copy `Configuration/test_config.py` to `Configuration/etlconfig.py`
   - Update with your local database credentials

### Running Tests Locally
```bash
# Run all tests
pytest

# Run smoke tests only
pytest -m smoke

# Run regression tests
pytest -m regression

# Run data quality tests
pytest -m dq

# Run with HTML report
pytest --html=Reports/test_report.html --self-contained-html
```

## CI/CD

Tests run automatically on:
- Push to main or develop branches
- Pull requests to main
- Manual trigger from GitHub Actions UI

## Test Categories

- **Smoke Tests**: Quick validation tests
- **Regression Tests**: Comprehensive functional tests
- **Data Quality Tests**: Duplicate checks, null checks, referential integrity
- **Integration Tests**: Database connectivity tests

## Contributing

1. Create a feature branch
2. Make changes
3. Run tests locally
4. Create pull request
5. Wait for CI tests to pass"# capstone_retail_dwh" 
