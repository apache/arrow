# Install built wheel
pip install /dist/*.whl

# Runs tests on installed distribution from an empty directory
python --version

# Test optional dependencies
python -c "import pyarrow"
python -c "import pyarrow.orc"
python -c "import pyarrow.parquet"
python -c "import pyarrow.plasma"

# Run pyarrow tests
pip install pytest pytest-faulthandler pandas
pytest --pyargs pyarrow
