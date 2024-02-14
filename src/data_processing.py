import re
from pyspark.sql.functions import (
    udf,
    col,
    avg,
    format_number,
    split,
    upper,
    regexp_replace,
    regexp_extract,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, Row


def extract_year_and_update_title(title):
    """Extracts the year from a movie title and updates the title.

    Args:
        title (str): The movie title.

    Returns:
        Row: A Row object containing 'year' (int) and 'title' (str).
    """
    try:
        match = re.search(r"\((\d{4})\)", title)
        if match:
            year = int(match.group(1))
            new_title = re.sub(r"\(\d{4}\)", "", title).strip()
            return Row(year=year, title=new_title)
        else:
            return Row(year=None, title=title)
    except Exception as e:
        print(f"Error processing title: {title}, Error: {e}")
        return Row(year=None, title=title)


def transform_data(movies_df, ratings_df):
    """Transforms and prepares the data for analysis.

    Args:
        movies_df (DataFrame): DataFrame containing movie information.
        ratings_df (DataFrame): DataFrame containing movie ratings.

    Returns:
        DataFrame: Processed and aggregated DataFrame ready for analysis or saving.
    """

    # Define the schema for the extracted year and title
    schema = StructType(
        [
            StructField("year", IntegerType(), nullable=True),
            StructField("title", StringType(), nullable=True),
        ]
    )

    # Register the UDF
    extract_year_and_update_title_udf = udf(extract_year_and_update_title, schema)

    # Extract year and update movie titles
    movies_df = movies_df.withColumn(
        "year",
        regexp_extract(col("title"), r"\((\d{4})\)", 1).cast(IntegerType()),
    )
    movies_df = movies_df.withColumn("title", regexp_replace(col("title"), r"\(\d{4}\)", ""))

    # Join movie information with ratings
    movies_ratings_df = movies_df.join(ratings_df, "movieId")

    # Calculate average ratings
    movies_ratings_df = movies_ratings_df.groupBy("movieId", "title", "genres", "year").agg(
        avg("rating").alias("average_rating")
    )

    # Format the average rating
    movies_ratings_df = movies_ratings_df.withColumn(
        "average_rating",
        format_number("average_rating", 2),
    )

    # Uppercase titles and genres
    movies_ratings_df = movies_ratings_df.withColumn("title", upper(col("title")))
    movies_ratings_df = movies_ratings_df.withColumn("genres", upper(col("genres")))

    # Split genres into an array
    movies_ratings_df = movies_ratings_df.withColumn("genres", split(col("genres"), "\|"))

    # Sort by average rating
    movies_ratings_df = movies_ratings_df.orderBy("average_rating", ascending=False)

    # Remove quotes from titles
    movies_ratings_df = movies_ratings_df.withColumn(
        "title",
        regexp_replace(col("title"), '^"|"$', ""),
    )

    return movies_ratings_df
