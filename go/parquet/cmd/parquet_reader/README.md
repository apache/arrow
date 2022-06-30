<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# parquet\_reader

A tool to read Parquet files and write selected columns into TEXT, JSON, or CSV files.

## Usage

```
$ ./parquet_reader -h
Parquet Reader (version 0.1.20220629.1846)
Usage:
  parquet_reader -h | --help
  parquet_reader [--only-metadata] [--no-metadata] [--no-memory-map] [--json] [--csv] [--output=FILE]
                 [--print-key-value-metadata] [--int96-timestamp] [--columns=COLUMNS] <file>
Options:
  -h --help                     Show this screen.
  --print-key-value-metadata    Print out the key-value metadata. [default: false]
  --only-metadata               Stop after printing metadata, no values.
  --no-metadata                 Do not print metadata.
  --output=FILE                 Specify output file for data. [default: -]
  --no-memory-map               Disable memory mapping the file.
  --int96-timestamp             Parse INT96 as TIMESTAMP for legacy support.
  --json                        Format output as JSON instead of text.
  --csv                         Format output as CSV instead of text.
  --columns=COLUMNS             Specify a subset of columns to print, comma delimited indexes.
```

# Examples

## Text
```
$ ./parquet_reader --no-metadata v0.7.1.parquet
carat             |cut               |color             |clarity           |depth             |table             |price             |x                 |y                 |z                 |__index_level_0__ |
0.230000          |Ideal             |E                 |SI2               |61.500000         |55.000000         |326               |3.950000          |3.980000          |2.430000          |0                 |
0.210000          |Premium           |E                 |SI1               |59.800000         |61.000000         |326               |3.890000          |3.840000          |2.310000          |1                 |
0.230000          |Good              |E                 |VS1               |56.900000         |65.000000         |327               |4.050000          |4.070000          |2.310000          |2                 |
0.290000          |Premium           |I                 |VS2               |62.400000         |58.000000         |334               |4.200000          |4.230000          |2.630000          |3                 |
0.310000          |Good              |J                 |SI2               |63.300000         |58.000000         |335               |4.340000          |4.350000          |2.750000          |4                 |
0.240000          |Very Good         |J                 |VVS2              |62.800000         |57.000000         |336               |3.940000          |3.960000          |2.480000          |5                 |
0.240000          |Very Good         |I                 |VVS1              |62.300000         |57.000000         |336               |3.950000          |3.980000          |2.470000          |6                 |
0.260000          |Very Good         |H                 |SI1               |61.900000         |55.000000         |337               |4.070000          |4.110000          |2.530000          |7                 |
0.220000          |Fair              |E                 |VS2               |65.100000         |61.000000         |337               |3.870000          |3.780000          |2.490000          |8                 |
0.230000          |Very Good         |H                 |VS1               |59.400000         |61.000000         |338               |4.000000          |4.050000          |2.390000          |9                 |
```

## JSON
```
$ ./parquet_reader --no-metadata --json v0.7.1.parquet
[{"carat":0.23,"cut":"Ideal","color":"E","clarity":"SI2","depth":61.5,"table":55,"price":326,"x":3.95,"y":3.98,"z":2.43,"__index_level_0__":0},{"carat":0.21,"cut":"Premium","color":"E","clarity":"SI1","depth":59.8,"table":61,"price":326,"x":3.89,"y":3.84,"z":2.31,"__index_level_0__":1},{"carat":0.23,"cut":"Good","color":"E","clarity":"VS1","depth":56.9,"table":65,"price":327,"x":4.05,"y":4.07,"z":2.31,"__index_level_0__":2},{"carat":0.29,"cut":"Premium","color":"I","clarity":"VS2","depth":62.4,"table":58,"price":334,"x":4.2,"y":4.23,"z":2.63,"__index_level_0__":3},{"carat":0.31,"cut":"Good","color":"J","clarity":"SI2","depth":63.3,"table":58,"price":335,"x":4.34,"y":4.35,"z":2.75,"__index_level_0__":4},{"carat":0.24,"cut":"Very Good","color":"J","clarity":"VVS2","depth":62.8,"table":57,"price":336,"x":3.94,"y":3.96,"z":2.48,"__index_level_0__":5},{"carat":0.24,"cut":"Very Good","color":"I","clarity":"VVS1","depth":62.3,"table":57,"price":336,"x":3.95,"y":3.98,"z":2.47,"__index_level_0__":6},{"carat":0.26,"cut":"Very Good","color":"H","clarity":"SI1","depth":61.9,"table":55,"price":337,"x":4.07,"y":4.11,"z":2.53,"__index_level_0__":7},{"carat":0.22,"cut":"Fair","color":"E","clarity":"VS2","depth":65.1,"table":61,"price":337,"x":3.87,"y":3.78,"z":2.49,"__index_level_0__":8},{"carat":0.23,"cut":"Very Good","color":"H","clarity":"VS1","depth":59.4,"table":61,"price":338,"x":4,"y":4.05,"z":2.39,"__index_level_0__":9}]
```

## CSV
```
$ ./parquet_reader --no-metadata --csv v0.7.1.parquet
"carat","cut","color","clarity","depth","table","price","x","y","z","__index_level_0__"
0.23,"Ideal","E","SI2",61.5,55,326,3.95,3.98,2.43,0
0.21,"Premium","E","SI1",59.8,61,326,3.89,3.84,2.31,1
0.23,"Good","E","VS1",56.9,65,327,4.05,4.07,2.31,2
0.29,"Premium","I","VS2",62.4,58,334,4.2,4.23,2.63,3
0.31,"Good","J","SI2",63.3,58,335,4.34,4.35,2.75,4
0.24,"Very Good","J","VVS2",62.8,57,336,3.94,3.96,2.48,5
0.24,"Very Good","I","VVS1",62.3,57,336,3.95,3.98,2.47,6
0.26,"Very Good","H","SI1",61.9,55,337,4.07,4.11,2.53,7
0.22,"Fair","E","VS2",65.1,61,337,3.87,3.78,2.49,8
0.23,"Very Good","H","VS1",59.4,61,338,4,4.05,2.39,9
```

## Write JSON to output file
```
$ ./parquet_reader --no-metadata --json --output=data.json v0.7.1.parquet
$ jq . data.json
[
  {
    "carat": 0.23,
    "cut": "Ideal",
    "color": "E",
    "clarity": "SI2",
    "depth": 61.5,
    "table": 55,
    "price": 326,
    "x": 3.95,
...
```

## Write CSV to output file
```
$ ./parquet_reader --no-metadata --csv --output=data.csv v0.7.1.parquet
```
