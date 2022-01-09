# .NET IoT Analytics

## Introduction

For Big Data problems, people will find that most of the technology and resources are for Java, Python, R, and Scala.
Unfortunately, it's not the same for C#. But with Apache Arrow, a language-agnostic data format, the gap between Big Data Technology and
.Net Enterprise Software Development can be bridged. 

## The original dataset is:

[WISDM Smartphone and Smartwatch Activity and Biometrics Dataset Dataset](https://archive.ics.uci.edu/ml/datasets/WISDM+Smartphone+and+Smartwatch+Activity+and+Biometrics+Dataset+):
   Contains accelerometer and gyroscope time-series sensor data collected from a smartphone and smartwatch as 51 test subjects perform 18 activities for 3 minutes each.

## The sample dataset used in this example is randomly generated in order to test arrow in large-scale data scenario

The sample dataset includes activity data from 1000 participants from an activity recognition project.
Each participant performed each of the 18 activities for a total amount of one billion accelerometer data events
reported from smartphone and smartwatch.

* Timestamp is the time at which the sensor reported the reading.
* X_Axis is the g-force acceleration along the x-axis.
* Y_Axis is the g-force acceleration along the y-axis.
* Z_Axis is the g-force acceleration along the z-axis.

## Persist data in Arrow format

The application has eight producer threads producing data and one consumer thread consumes and transforms data, since the data volume is huge,
intermediate results (approximately 100 million records) need to be checkpointed into Arrow record batch, and finally persisted together to disk in .arrow data format,
then further analysis can be performed in high-level Arrow computing APIs.
