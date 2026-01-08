# MovieLens – Spark ETL

This folder contains the Spark ETL script that builds a **silver** dataset from MovieLens CSV files using Apache Spark (local mode).

## Output schema (silver)
The final dataset contains:
- `movieId`
- `movieName`
- `year_of_release`
- `number_ratings`
- `avg_rating`
- `genre`  *(1 row per genre)*

## Project structure
Recommended layout:
# MovieLens – Spark ETL (Member 1) – Local run

This folder contains the Spark ETL script that builds a **silver** dataset from MovieLens CSV files using Apache Spark (local mode).

## Output schema (silver)
The final dataset contains:
- `movieId`
- `movieName`
- `year_of_release`
- `number_ratings`
- `avg_rating`
- `genre`  *(1 row per genre)*

## Project structure
Recommended layout:

# MovieLens – Spark ETL (Member 1) – Local run

This folder contains the Spark ETL script that builds a **silver** dataset from MovieLens CSV files using Apache Spark (local mode).

## Output schema (silver)
The final dataset contains:
- `movieId`
- `movieName`
- `year_of_release`
- `number_ratings`
- `avg_rating`
- `genre`  *(1 row per genre)*

## Project structure
Recommended layout:
movielens/
├── spark_ETL.py
├── raw/
│ ├── movies.csv
│ └── ratings.csv
└── silver/ spark/ # Parquet output (_SUCCESS + part-*.parquet)

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
  file:///Users/<YOUR_USER>/.../movielens/silver/spark
