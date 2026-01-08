import argparse # pour recevoir les paramètres comme input et output
from pyspark.sql import SparkSession
from pyspark.sql.functions import(col, regexp_extract, split, explode, count, avg, trim, round, when, length)


#Créer un function pour lire les paramètres de la ligne de commande (input and output)
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input path (file:/// or hdfs:///)")
    parser.add_argument("--output", required=True, help="Output path (file:/// or hdfs:///)")
    return parser.parse_args() # retourner args.input et args.output

def extract(spark, input_path):
    # movies dataset
    movies_df = (
        spark.read
        .option("header", True)
        .option("inferSchema",True)
        .csv(f"{input_path}/movies.csv")
    )
    # ratings dataset
    ratings_df = (
       spark.read
       .option("header", True)
       .option("inferSchema", True)
       .csv(f"{input_path}/ratings.csv")

   )

    return movies_df, ratings_df

def transform(movies_df,ratings_df):
    # year_of_release - extract year and convert to int safely
    year_extracted = regexp_extract(col("title"), r"\((\d{4})\)", 1)
    movies_tranf_df = movies_df.withColumn(
        "year_of_release",
        when(length(year_extracted) == 4, year_extracted.cast("int")).otherwise(None)
        )

    #genre
    movies_tranf_df = (
        movies_tranf_df 
        .withColumn("genre",explode(split(col("genres"),r"\|"))) # split genre in array and make each element into separate line
        .withColumn("genre", trim(col("genre"))) #remove space ex " Action " to "Action"
    )

    #calcul average rating
    ratings_tranf_df = (
        ratings_df
        .groupBy("movieId")
        .agg(
            count("*").alias("number_ratings"),
            round(avg(col("rating")),2).alias("avg_rating")
        )
    )  

    # final dataframe (join 2 tables)
    final_df = (
        movies_tranf_df
        .join(ratings_tranf_df, on="movieId",how="left")
        .select(
            col("movieId"),
            col("title").alias("movieName"),
            col("year_of_release"),
            col("number_ratings"),
            col("avg_rating"),
            col("genre")
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

# Write to CSV with proper configuration
    (final_df.write
        .mode("overwrite")
        .option("header", "true")
        .parquet(args.output))

    print("Data successfully written to:", args.output)

    spark.stop()

if __name__ == "__main__":
    main()
    