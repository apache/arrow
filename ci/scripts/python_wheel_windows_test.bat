set PYARROW_TEST_CYTHON=OFF
set PYARROW_TEST_DATASET=ON
set PYARROW_TEST_GANDIVA=OFF
set PYARROW_TEST_HDFS=ON
set PYARROW_TEST_ORC=OFF
set PYARROW_TEST_PANDAS=ON
set PYARROW_TEST_PARQUET=ON
set PYARROW_TEST_PLASMA=OFF
set PYARROW_TEST_S3=OFF
set PYARROW_TEST_TENSORFLOW=ON
set PYARROW_TEST_FLIGHT=ON

set ARROW_TEST_DATA=C:\arrow\testing\data
set PARQUET_TEST_DATA=C:\arrow\submodules\parquet-testing\data

@REM Install the built wheels
python -m pip install numpy
python -m pip install --no-index --find-links=C:\arrow\python\dist\ pyarrow || exit /B

@REM Test that the modules are importable
python -c "import pyarrow"
python -c "import pyarrow._hdfs"
python -c "import pyarrow._s3fs"
python -c "import pyarrow.csv"
python -c "import pyarrow.dataset"
python -c "import pyarrow.flight"
python -c "import pyarrow.fs"
python -c "import pyarrow.json"
python -c "import pyarrow.parquet"

@REM Install testing dependencies
pip install -r C:\arrow\python\requirements-wheel-test.txt

@REM Execute unittest
pytest -r s --pyargs pyarrow
