MovieLens – Spark ETL

This folder contains the Spark ETL script that builds a **silver** dataset from MovieLens CSV files using Apache Spark.

## Output schema (silver)
The final dataset contains:
- `movieId`
- `movieName`
- `year_of_release`
- `number_ratings`
- `avg_rating`
- `genre`  *(1 row per genre)*


# MovieLens – Spark ETL – Local run

## Project structure
Recommended layout:
movielens/
├── spark_ETL.py
├── raw/
│ ├── movies.csv
│ └── ratings.csv
└── silver/ Parquet output (_SUCCESS + part-*.parquet)

## Prerequisites (local)
- **Java**: JDK 11+ (Spark runs on JVM)
- **Apache Spark**: installed locally (e.g., via Homebrew on macOS)
- **Python**: 3.9+

  Check:
  ```bash
  java -version
  spark-submit --version
  python3 --version

  # Run ETL (local)
  spark-submit spark_ETL.py \
    file:///Users/<YOUR_USER>/.../movielens/raw \
    file:///Users/<YOUR_USER>/.../movielens/silver

# MovieLens – Spark ETL – Hadoop cluster RUN

  1. Cluster setup (Docker) Start containers:  

  docker start hadoop-master hadoop-worker1 hadoop-worker2

  2. Copy data from local machine to Hadoop master (HOST)

  docker cp <local_path>/movies.csv hadoop-master:/tmp/movies.csv

  docker cp <local_path>/ratings.csv hadoop-master:/tmp/ratings.csv
  
  example:

  docker cp /Users/linhle/LinhLe/spark_project/movielens/raw/movies.csv hadoop-master:/tmp/movies.csv

  docker cp /Users/linhle/LinhLe/spark_project/movielens/raw/ratings.csv hadoop-master:/tmp/ratings.csv

  3. Enter Hadoop master container

  docker exec -it hadoop-master bash

4. Start Hadoop services

start-dfs.sh

start-yarn.sh

5. Verify Hadoop services

jps

6. Verify data inside container

ls -lh /tmp/movies.csv /tmp/ratings.csv

7. Load data into HDFS (RAW)

hdfs dfs -mkdir -p /data/movielens/raw

hdfs dfs -put -f /tmp/movies.csv /data/movielens/raw/

hdfs dfs -put -f /tmp/ratings.csv /data/movielens/raw/

Verify: 

hdfs dfs -ls /data/movielens/raw

8. Copy Spark ETL script to Hadoop master (HOST)

Exit container: exit

docker cp <local_path>/spark_ETL.py hadoop-master:/tmp/spark_ETL.py

9. Run Spark ETL job (INSIDE container)

docker exec -it hadoop-master bash

spark-submit --master local[*] /tmp/spark_ETL.py \
  --input hdfs:///data/movielens/raw \
  --output hdfs:///data/movielens/silver

10. Verify Spark output in HDFS

hdfs dfs -ls /data/movielens/silver
