# DBLP-MapReduce-Analysis
## Overview
This project focuses on analyzing the DBLP (Digital Bibliography & Library Project) dataset using MapReduce algorithms implemented in Java. The dataset, obtained from the DBLP website, contains bibliographic information about computer science articles. Two distinct MapReduce algorithms are designed to answer specific questions about the dataset.

## Question 1: Journal Publication Statistics
### Algorithm Overview
### Objective:

Determine the number of articles published in each journal per year in the DBLP articles dataset.
#### Map Stage:

* Key:
Composite key of (Journal/Booktitle, Year).
* Value:
Constant value of 1 to signify the existence of an article for the corresponding key.
#### Reduce Stage:

* Key:
Journal/Booktitle.
* Value:
Aggregate count of articles published in that journal for each year.
## Question 2: Co-authorship Graph
### Algorithm Overview
### Objective:

Construct a co-authorship graph from the DBLP dataset.
Nodes represent authors, and edges represent co-authorships with edge weights as the number of publications.
#### Map Stage:

* Key:
Author pairs sorted lexicographically.
* Value:
Constant value of 1 to signify the co-authorship between the author pair.
#### Reduce Stage:

* Key:
Author pair.
* Value:
Aggregate count of co-authored publications.
### Getting Started
* Clone the Repository:

git clone https://github.com/your-username/dblp-mapreduce-analysis.git
### Dataset Download:

Download the DBLP dataset from DPLB and place it in the project directory.
### Build and Run:

Navigate to the project directory and compile the Java code.
javac -cp .:path/to/hadoop-common.jar:other/hadoop/jars -d . YourMapper.java YourReducer.java YourDriver.java
Run the MapReduce job.
hadoop jar your-jar-file.jar YourDriver input-path output-path
### Dependencies
Apache Hadoop
### Contributing
Feel free to contribute by opening issues or submitting pull requests. Your feedback and improvements are highly appreciated.
