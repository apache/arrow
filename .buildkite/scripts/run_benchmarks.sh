/arrow/ci/scripts/cpp_build.sh /arrow /build &&
/arrow/ci/scripts/python_build.sh /arrow /build &&
/arrow/ci/scripts/python_test.sh /arrow

cd /workspace/conbench/
pip install -r requirements-cli.txt
pip install -U PyYAML
python setup.py install

cd /workspace/ursa-qa/python/
python setup.py develop

cd /workspace/ursa-qa/python/benchmarks/
conbench --help

touch /workspace/ursa-qa/python/benchmarks/.conbench
echo "url: https://conbench.ursa.dev" >> /workspace/ursa-qa/python/benchmarks/.conbench
echo "email: elena@ursacomputing.com" >> /workspace/ursa-qa/python/benchmarks/.conbench
echo "password: test" >> /workspace/ursa-qa/python/benchmarks/.conbench

conbench feather-read nyctaxi_sample --iterations=10 --all=true