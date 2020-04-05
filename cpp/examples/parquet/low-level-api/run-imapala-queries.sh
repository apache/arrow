source $IMPALA_HOME/bin/impala-config.sh
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /panos/import_dir/UNCOMPRESSED

$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/1M
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/10M
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/25M
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/15M


$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_1M.parquet /panos/import_dir/UNCOMPRESSED/1M/all-types-1M.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_10M.parquet /panos/import_dir/UNCOMPRESSED/10M/all-types-10M.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_25M.parquet /panos/import_dir/UNCOMPRESSED/25M/all-types-25M.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_15M.parquet /panos/import_dir/UNCOMPRESSED/15M/all-types-15M.parquet

$HADOOP_HOME/bin/hdfs dfs -ls -R /panos/import_dir/UNCOMPRESSED


$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_1M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_10M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_25M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_15M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_50M";

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_1M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/1M/all-types-1M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/1M';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_10M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/10M/all-types-10M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/10M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_25M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/25M/all-types-25M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/25M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_15M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/15M/all-types-15M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/15M';"

$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_1M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_10M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_25M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_15M;"

$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /panos/export_dir/UNCOMPRESSED/
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/export_dir/UNCOMPRESSED/

$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_1M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_10M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_25M";

$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_15M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_50M";

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_1M LIKE all_types_table_u_1M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/1M';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_10M LIKE all_types_table_u_10M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/10M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_25M LIKE all_types_table_u_25M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/25M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_15M LIKE all_types_table_u_15M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/15M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_1M SELECT * FROM all_types_table_u_1M;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_10M SELECT * FROM all_types_table_u_10M;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_25M SELECT * FROM all_types_table_u_25M;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_15M SELECT * FROM all_types_table_u_15M;"

$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_1M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_10M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_25M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_15M;"

$HADOOP_HOME/bin/hdfs dfs -ls -R /panos/export_dir/UNCOMPRESSED

mkdir -p ~/parquet_data/testing/exports/UNCOMPRESSED/

hadoop fs -get /panos/export_dir/UNCOMPRESSED/*   ~/parquet_data/testing/exports/UNCOMPRESSED/

ls -laR ~/parquet_data/testing/exports/UNCOMPRESSED/
