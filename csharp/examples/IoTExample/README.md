# Arrow and .NET IoT In-Memory Analytics

## Problems

For Big Data problems, people will find that most of the technology and resources are for Java, Python, R, and Scala.
Unfortunately, it's not the same for C#. But with Apache Arrow, a language-agnostic data format, the gap between Big Data Technology and
.Net Enterprise Software Development can be bridged. 

## The original dataset used in the example is:

[WISDM Smartphone and Smartwatch Activity and Biometrics Dataset Dataset](https://archive.ics.uci.edu/ml/datasets/WISDM+Smartphone+and+Smartwatch+Activity+and+Biometrics+Dataset+):
   Contains accelerometer and gyroscope time-series sensor data collected from a smartphone and smartwatch as 51 test subjects perform 18 activities for 3 minutes each.

## The actual dataset in this example is randomly generated in order to meet large-scale data scenario

The dataset includes activity data from 1000 participants from an activity recognition project.
Each participant performed each of the 18 activities for a total amount of one billion sensor data (accelerometer 
and gyroscope for smartphone and smartwatch).

## Persist data in Arrow format

One producer thread produces data and one consumer thread consumes and transforms data, since the data volume is huge,
the intermediate results (approximately 100 million records) need to be checkpointed and persisted to disk in Arrow format,
and the size of each Arrow file is about 1.66 GB.
