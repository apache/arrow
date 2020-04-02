source $IMPALA_HOME/bin/impala-config.sh
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /panos/import_dir/UNCOMPRESSED

$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/10k
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/100k
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/1M
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/10M
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/100M
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/200M
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/500M
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/import_dir/UNCOMPRESSED/1B

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_10k.parquet /panos/import_dir/UNCOMPRESSED/10k/all-types-10k.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_100k.parquet /panos/import_dir/UNCOMPRESSED/100k/all-types-100k.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_1M.parquet /panos/import_dir/UNCOMPRESSED/1M/all-types-1M.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_10M.parquet /panos/import_dir/UNCOMPRESSED/10M/all-types-10M.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_100M.parquet /panos/import_dir/UNCOMPRESSED/100M/all-types-100M.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_200M.parquet /panos/import_dir/UNCOMPRESSED/200M/all-types-200M.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_500M.parquet /panos/import_dir/UNCOMPRESSED/500M/all-types-500M.parquet

$HADOOP_HOME/bin/hdfs dfs -copyFromLocal ~/parquet_data/testing/imports/UNCOMPRESSED/parquet_cpp_example_1B.parquet /panos/import_dir/UNCOMPRESSED/1B/all-types-1B.parquet

$HADOOP_HOME/bin/hdfs dfs -ls -R /panos/import_dir/UNCOMPRESSED


$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_10k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_100k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_1M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_10M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_100M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_200M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_500M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE all_types_table_u_1B";

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_10k LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/10k/all-types-10k.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/10k';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_100k LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/100k/all-types-100k.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/100k';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_1M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/1M/all-types-1M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/1M';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_10M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/10M/all-types-10M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/10M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_100M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/100M/all-types-100M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/100M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_200M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/200M/all-types-200M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/200M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_500M LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/500M/all-types-500M.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/500M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;CREATE EXTERNAL TABLE all_types_table_u_1B LIKE PARQUET '/panos/import_dir/UNCOMPRESSED/1B/all-types-1B.parquet' STORED AS PARQUET LOCATION '/panos/import_dir/UNCOMPRESSED/1B';"

$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_10k;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_100k;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_1M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_10M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_100M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_200M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_500M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from all_types_table_u_1B;"

$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash /panos/export_dir/UNCOMPRESSED/
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /panos/export_dir/UNCOMPRESSED/

$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_10k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_100k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_1M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_10M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_100M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_200M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_500M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_u_1B";

$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_10k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_100k";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_1M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_10M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_100M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_200M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_500M";
$IMPALA_HOME/bin/impala-shell.sh -q "DROP TABLE export_all_types_table_1B";

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_10k LIKE all_types_table_u_10k STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/10k';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_100k LIKE all_types_table_u_100k STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/100k';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_1M LIKE all_types_table_u_1M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/1M';"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_10M LIKE all_types_table_u_10M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/10M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_100M LIKE all_types_table_u_100M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/100M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_200M LIKE all_types_table_u_200M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/200M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_500M LIKE all_types_table_u_500M STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/500M';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;create table export_all_types_table_u_1B LIKE all_types_table_u_1B STORED AS PARQUET LOCATION '/panos/export_dir/UNCOMPRESSED/1B';"

$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_10k SELECT * FROM all_types_table_u_10k;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_100k SELECT * FROM all_types_table_u_100k;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_1M SELECT * FROM all_types_table_u_1M;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_10M SELECT * FROM all_types_table_u_10M;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_100M SELECT * FROM all_types_table_u_100M;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_200M SELECT * FROM all_types_table_u_200M;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_500M SELECT * FROM all_types_table_u_500M;"
$IMPALA_HOME/bin/impala-shell.sh -q "SET COMPRESSION_CODEC=NONE;set PARQUET_FILE_SIZE=1g;INSERT INTO TABLE  export_all_types_table_u_1B SELECT * FROM all_types_table_u_1B;"

$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_10k;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_100k;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_1M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_10M;"

$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_100M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_200M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_500M;"
$IMPALA_HOME/bin/impala-shell.sh -q "select count(*) from export_all_types_table_u_1B;"

$HADOOP_HOME/bin/hdfs dfs -ls -R /panos/export_dir/UNCOMPRESSED

mkdir -p ~/parquet_data/testing/exports/UNCOMPRESSED/

hadoop fs -get /panos/export_dir/UNCOMPRESSED/*   ~/parquet_data/testing/exports/UNCOMPRESSED/

ls -laR ~/parquet_data/testing/exports/UNCOMPRESSED/
