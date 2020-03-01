source $IMPALA_HOME/bin/impala-config.sh
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /panos/import_dir/UNCOMPRESSED

$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/10k
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/100k
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/1M
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/10M

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_10k.parquet /panos/import_dir/UNCOMPRESSED/10k/all-types-10k.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_100k.parquet /panos/import_dir/UNCOMPRESSED/100k/all-types-100k.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_1M.parquet /panos/import_dir/UNCOMPRESSED/1M/all-types-1M.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_10M.parquet /panos/import_dir/UNCOMPRESSED/10M/all-types-10M.parquet

$HADOOP_HOME/bin/hdfs dfs -ls -R /panos/import_dir/UNCOMPRESSED


$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_10k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_100k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_1M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_10M";

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;CREATE EXTERNAL TABLE all_types_table_u_10k LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/10k/all-types-10k.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/10k';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;CREATE EXTERNAL TABLE all_types_table_u_100k LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/100k/all-types-100k.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/100k';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;CREATE EXTERNAL TABLE all_types_table_u_1M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/1M/all-types-1M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/1M';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;CREATE EXTERNAL TABLE all_types_table_u_10M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/10M/all-types-10M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/10M';"

$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_10k;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_100k;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_1M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_10M;"

$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /panos/export_dir/UNCOMPRESSED/
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/export_dir/UNCOMPRESSED/

$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_10k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_100k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_1M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_10M";

$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_10k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_100k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_1M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_10M";

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;create table export_all_types_table_u_10k LIKE all_types_table_u_10k STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/10k';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;create table export_all_types_table_u_100k LIKE all_types_table_u_100k STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/100k';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;create table export_all_types_table_u_1M LIKE all_types_table_u_1M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/1M';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;create table export_all_types_table_u_10M LIKE all_types_table_u_10M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/10M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;INSERT INTO TABLE  export_all_types_table_u_10k SELECT * FROM all_types_table_u_10k;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;INSERT INTO TABLE  export_all_types_table_u_100k SELECT * FROM all_types_table_u_100k;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;INSERT INTO TABLE  export_all_types_table_u_1M SELECT * FROM all_types_table_u_1M;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;INSERT INTO TABLE  export_all_types_table_u_10M SELECT * FROM all_types_table_u_10M;"

$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_10k;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_100k;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_1M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_10M;"


$HADOOP_HOME/bin/hdfs dfs -ls -R /panos/export_dir/UNCOMPRESSED


hadoop fs -get /panos/export_dir/UNCOMPRESSED/*   ~/parquet_data/testing/exports/UNCOMPRESSED/

ls -laR ~/parquet_data/testing/exports/UNCOMPRESSED/
