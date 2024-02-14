from pyspark.sql.utils import AnalysisException


def write_to_postgres(df, config):
    """Writes a Spark DataFrame to a PostgreSQL database.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to write.
        config (dict): A dictionary containing database connection details:
            - url: The JDBC URL of the PostgreSQL database.
            - user: The database username.
            - password: The database password.
            - driver: The JDBC driver class (usually 'org.postgresql.Driver').
    """

    url = config["url"]
    properties = {
        "user": config["user"],
        "password": config["password"],
        "driver": "org.postgresql.Driver",
    }

    try:
        df.write.jdbc(url=url, table="movies_ratings", mode="overwrite", properties=properties)
    except AnalysisException as e:
        print(f"Failed to write data to PostgreSQL: {e}")
