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

  2. Enter Container:

  docker exec -it hadoop-master bash

  3. Start Hadoop services:

  start-dfs.sh

  start-yarn.sh

  4. Verify services:

  jps

  Check if you see : NameNode, SecondaryNameNode, ResourceManager

  5. Copy data from local to container hadoop-master:

  docker cp < your local path > hadoop-master:/tmp/movies.csv

  docker cp < your local path > hadoop-master:/tmp/ratings.csv

  example: docker cp /Users/linhle/LinhLe/spark_project/movielens/raw/movies.csv hadoop-master:/tmp/movies.csv

  6. Verify if data is loaded in container:

  docker exec -it hadoop-master bash -lc "ls -lh /tmp/movies.csv /tmp/ratings.csv"

  docker exec -it hadoop-master bash -lc "ls -lh /tmp/movies.csv /tmp/movies.csv"

  7. Create folder in HDFS ( you should be inside container):

  hdfs dfs -mkdir -p /user/root/movielens/raw &&

  hdfs dfs -put -f /tmp/movies.csv /user/root/movielens/raw/ &&

  hdfs dfs -put -f /tmp/ratings.csv /user/root/movielens/raw/ &&

  hdfs dfs -ls /user/root/movielens/raw

  8. Verify the output ( check if the data is stored in HDFS):

  hdfs dfs -ls /data/movielens/raw

  9. Copy pipeline (spark_ETL.py) to container hadoop-master:

  docker cp < your local path > hadoop-master:/tmp/spark_ETL.py

  example:
  docker cp /Users/linhle/LinhLe/spark_project/movielens/spark_ETL.py hadoop-master:/tmp/spark_ETL.py

  10. Run job :

  spark-submit --master local[*] /tmp/spark_ETL.py \
  --input hdfs:///data/movielens/raw \
  --output hdfs:///data/movielens/silver

  11. Verify :
  
  hdfs dfs -ls /data/movielens/silver
  




