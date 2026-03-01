# ⚡ Big Data Analytics with Apache Spark & Scala

A robust, functional programming-based data analytics pipeline built entirely on **Apache Spark** using **Scala**. 

Developed for the **"Functional Programming, Analytics and Applications" (INF424)** course at the Technical University of Crete (TUC), this project demonstrates the processing of large-scale, semi-structured, and structured datasets utilizing Spark's resilient distributed datasets (RDDs) and Dataframes.

## 🚀 Project Overview

The project is divided into two major application scenarios. *Note: Pure functional transformations and higher-order functions are utilized throughout the project. The use of standard `spark.sql` string queries was strictly prohibited to enforce functional programming paradigms.*

### Scenario 1: Reuters News Stories Analysis (Spark RDDs)
This scenario analyzes a massive, semi-structured dataset of Reuters news stories to extract the relevance of specific terms to predefined news categories. 
* **Data Structures Used:** Spark RDDs
* **Objective:** Calculate the relevance between every term ($T$) and category ($C$) across millions of records.
* **Metric:** **Jaccard Index**, computed using the formula:
  $$JaccardIndex(T,C) = \frac{|DOC(T) \cap DOC(C)|}{|DOC(T) \cup DOC(C)|}$$
* **Output:** A delimited file stored in HDFS mapping `<category name>;<stemid>;<JaccardIndex>`.

### Scenario 2: Aegean Sea Vessel Trajectory Analytics (Spark Dataframes)
This scenario performs structured data analysis on vessel position tracking logs (NMEA) collected from stations in Piraeus and Syros Island.
* **Data Structures Used:** Spark Dataframes
* **Objective:** Answer complex trajectory and behavior queries based on timestamps, speeds (SOG), courses (COG), and statuses.
* **Resolved Queries:**
  1. Tracked vessel positions per station per day.
  2. The vessel ID (MMSI) with the highest number of tracked positions.
  3. Average Speed Over Ground (SOG) of vessels appearing in both station 8006 and station 10003 on the exact same day.
  4. Average absolute difference between Heading and Course Over Ground (COG) per station.
  5. The Top-3 most frequent vessel navigational statuses.

## 💻 Tech Stack
* **Language:** Scala
* **Framework:** Apache Spark
* **Storage:** HDFS (Hadoop Distributed File System)
* **Core APIs:** Spark RDD API, Spark DataFrame API

## 📁 Dataset Note (Data Not Included)
**Please note that the raw datasets are NOT included in this repository.**
* **File Size Limits:** The raw data files are too large to be hosted directly on GitHub.
* **Data Privacy / Licensing:** The Aegean Sea Vessel dataset (`nmea_aegean.logs`) was provided by the SmartMove laboratory exclusively for academic purposes and is restricted from public sharing. 
* **To run the code:** You will need to obtain the datasets independently and load them into your own HDFS environment before execution.
