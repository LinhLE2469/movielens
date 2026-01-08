import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, split, explode, count, avg, trim

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input path (file:// or hdfs://)")
    parser.add_argument("--output", required=True, help="Output path (file:// or hdfs://)")
    return parser.parse_args()

def extract(spark, input_path):
    movies_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{input_path}/movies.csv")
    )
    ratings_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{input_path}/ratings.csv")
    )
    return movies_df, ratings_df

def transform(movies_df, ratings_df):
    movies_t = movies_df.withColumn(
        "year",
        regexp_extract(col("title"), r"\((\d{4})\)", 1).cast("int")
    )
    movies_t = (
        movies_t
        .withColumn("genre", explode(split(col("genres"), r"\|")))
        .withColumn("genre", trim(col("genre")))
    )
    ratings_agg = (
        ratings_df
        .groupBy("movieId")
        .agg(
            count("*").alias("num_ratings"),
            avg(col("rating")).alias("avg_rating")
        )
    )
    final_df = (
        movies_t
        .join(ratings_agg, on="movieId", how="left")
        .select(
            col("movieId"),
            col("title").alias("movieName"),
            col("year"),
            col("num_ratings"),
            col("genre"),
            col("avg_rating")
        )
    )
    return final_df

def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("MovieLens Spark ETL")
        .getOrCreate()
    )
    movies_df, ratings_df = extract(spark, args.input)
    final_df = transform(movies_df, ratings_df)
    final_df.write.mode("overwrite").parquet(args.output)
    print("Rows written:", final_df.count())
    spark.stop()

if __name__ == "__main__":
    main()