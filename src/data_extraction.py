def load_data(spark, data_path):
    """Loads data from a specified path into a Spark DataFrame.

    Args:
        spark (SparkSession): Active SparkSession instance.
        data_path (str): Path to the data source.
        file_format (str, optional): Format of the data ('csv', 'json', 'parquet', etc.).
                                     Defaults to 'csv'.

    Returns:
        pyspark.sql.DataFrame: The loaded DataFrame.
    """

    try:
        reader = spark.read.format("csv").option("header", True)
    except ValueError as e:
        raise ValueError(f"Unsupported file format: {e}")

    return reader.load(data_path)


def load_movies(spark, data_path):
    """Loads the movies data, selecting specified columns.

    Args:
        spark (SparkSession): Active SparkSession instance.
        data_path (str): Path to the 'movies.csv' file.

    Returns:
        pyspark.sql.DataFrame: DataFrame containing relevant columns from 'movies.csv'.
    """

    movies_df = load_data(spark, data_path)
    return movies_df.select("movieId", "title", "genres")


def load_ratings(spark, data_path):
    """Loads the ratings data.

    Args:
        spark (SparkSession): Active SparkSession instance.
        data_path (str): Path to the 'ratings.csv' file.

    Returns:
        pyspark.sql.DataFrame: DataFrame containing 'movieId' and 'rating' columns.
    """

    ratings_df = load_data(spark, data_path)
    return ratings_df.select("movieId", "rating")
