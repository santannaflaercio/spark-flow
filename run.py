import os
import yaml
import dotenv
import logging

from src.data_extraction import load_movies, load_ratings
from src.data_processing import transform_data
from src.database_interaction import write_to_postgres
from pyspark.sql import SparkSession


def main():
    """Coordinates the ETL pipeline steps: data extraction, transformation, and loading into a database."""

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Load Configuration
    with open("config/config.yml", "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(f"Error loading configuration file: {exc}")
            return

    dotenv.load_dotenv()  # Load environment variables

    # Initialize Spark Session
    spark = (
        SparkSession.builder.appName("Movie Analysis")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.3,org.postgresql:postgresql:42.5.0")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .getOrCreate()
    )

    # Extract Data
    logging.info("Extracting data...")
    movies_df = load_movies(spark, f"{config['aws']['s3_endpoint']}/movies.csv")  # Assume S3
    ratings_df = load_ratings(spark, f"{config['aws']['s3_endpoint']}/ratings.csv")
    logging.info("Data extraction completed.")

    # Process Data
    logging.info("Processing data...")
    processed_df = transform_data(movies_df, ratings_df)
    logging.info("Data processing completed.")

    # Load Data into PostgreSQL
    logging.info("Loading data into PostgreSQL...")
    database_config = config["database"]
    write_to_postgres(processed_df, database_config)
    logging.info("Data loading completed.")

    spark.stop()  # Release Spark resources


if __name__ == "__main__":
    main()
