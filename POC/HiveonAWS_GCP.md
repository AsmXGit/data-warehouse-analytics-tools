# Hive Data Summarization and Analysis POC

## Overview

This tutorial demonstrates Hive's capabilities for easy data summarization and ad-hoc analysis of large volumes of data using Hive-QL.

## Objectives

1. Load data into Hive tables.
2. Create tables using RCFile and ORC formats.
3. Query tables.
4. Understand managed vs. external tables.
5. Partition and bucket tables.

## Prerequisites

- AWS EMR or GCP Dataproc cluster.
- Hive installed.
- Twitter data file (`TwitterData.txt`).

## Step 1: Load Data into Hive Table

1. Download `TwitterData.txt` from (link unavailable).
2. Upload the file to HDFS (`/tmp`).
3. Create a Hive table using Ambari UI or the command line:

```sql
CREATE TABLE TwitterExample (
  tweetId BIGINT,
  username STRING,
  txt STRING,
  CreatedAt STRING,
  profileLocation STRING,
  favc BIGINT,
  retweet STRING,
  retcount BIGINT,
  followerscount BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
```

4. Load data into the table:

```sql
LOAD DATA INPATH '/tmp/Twitterdata.txt' OVERWRITE INTO TABLE TwitterExample;
```

## Step 2: Create Table using RCFile Format

```sql
CREATE TABLE TwitterExampleRC (
  tweetId BIGINT,
  username STRING,
  txt STRING,
  CreatedAt STRING,
  profileLocation STRING,
  favc BIGINT,
  retweet STRING,
  retcount BIGINT,
  followerscount BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS RCFILE;
```

## Step 3: Query Tables

```sql
SELECT profileLocation, COUNT(txt) AS count1 
FROM TwitterExample 
GROUP BY profileLocation 
ORDER BY count1 DESC 
LIMIT 10;
```

## Step 4: Managed vs. External Tables

- **Managed Tables**: 
  ```sql
  CREATE TABLE ManagedExample ...
  ```

- **External Tables**: 
  ```sql
  CREATE EXTERNAL TABLE ExternalExample ...
  ```

## Step 5: ORC File Format

```sql
CREATE TABLE ORCFileFormatExample (
  tweetId BIGINT,
  username STRING,
  txt STRING,
  CreatedAt STRING,
  profileLocation STRING,
  favc INT,
  retweet STRING,
  retcount INT,
  followerscount INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS ORC 
TBLPROPERTIES ("orc.compress"="ZLIB");
```

## Step 6: Partitioned Table

```sql
CREATE TABLE PARTITIONEDExample (
  tweetId BIGINT,
  username STRING,
  txt STRING,
  favc BIGINT,
  retweet STRING,
  retcount BIGINT,
  followerscount BIGINT
) PARTITIONED BY (CreatedAt STRING, profileLocation STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
```

## Step 7: Bucketing

```sql
CREATE TABLE BucketingExample (
  tweetId BIGINT,
  username STRING,
  txt STRING,
  CreatedAt STRING,
  favc BIGINT,
  retweet STRING,
  retcount BIGINT,
  followerscount BIGINT
) PARTITIONED BY (profileLocation STRING) 
CLUSTERED BY (tweetId) INTO 2 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
```

This tutorial provides a comprehensive guide to leveraging Hive for data analysis and management, enabling users to efficiently process large datasets.
