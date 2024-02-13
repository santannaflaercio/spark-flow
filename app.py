import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, format_number, split, upper, regexp_replace
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, Row

# Inicie uma sessão Spark com acesso ao S3
spark = (
    SparkSession.builder.appName("Acesso ao S3 com PySpark")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.3")
    .getOrCreate()
)

# Lendo arquivos CSV do S3
movies_df = spark.read.csv("data/movies.csv", header=True)
# Lendo e selecionando colunas específicas do arquivo CSV
ratings_df = spark.read.csv("data/ratings.csv", header=True).select("movieId", "rating")

# Defina o esquema para o tipo complexo retornado pela UDF
schema = StructType(
    [
        StructField("year", IntegerType(), nullable=True),
        StructField("title", StringType(), nullable=True),
    ]
)


# Função para extrair o ano do título do filme e atualizar o título
def extract_year_and_update_title(title):
    try:
        match = re.search(r"\((\d{4})(?=\D|\))", title)
        if match:
            year = int(match.group(1))
            new_title = re.sub(r"\(\d{4}(?=\D|\))\)", "", title).strip()
            return Row(year=year, title=new_title)
        else:
            return Row(year=None, title=title)
    except Exception as e:
        # Log the exception and return None or a default value
        print(f"Error processing title: {title}, Error: {e}")
        return Row(year=None, title=title)


# Registre a função como uma UDF
extract_year_and_update_title_udf = udf(extract_year_and_update_title, schema)

# Aplicando a UDF para criar novas colunas 'year' e 'title'
movies_df = movies_df.withColumn("year_title", extract_year_and_update_title_udf(col("title")))
movies_df = movies_df.withColumn("year", col("year_title.year"))
movies_df = movies_df.withColumn("title", col("year_title.title"))
movies_df = movies_df.drop("year_title")

# Mescla os DataFrames 'movies_df' e 'ratings_df'
movies_ratings_df = movies_df.join(ratings_df, "movieId")

# Agrupa pelo 'movieId' e calcula a média das avaliações
movies_ratings_df = movies_ratings_df.groupBy("movieId", "title", "genres", "year").agg(
    avg("rating").alias("average_rating")
)

# Arredonda a média para 2 casas decimais
movies_ratings_df = movies_ratings_df.withColumn("average_rating", format_number("average_rating", 2))

# Convertendo os valores das colunas 'title' e 'genres' para maiúsculas
movies_ratings_df = movies_ratings_df.withColumn("title", upper(col("title")))
movies_ratings_df = movies_ratings_df.withColumn("genres", upper(col("genres")))

# Convertendo a string de gêneros em uma lista
movies_ratings_df = movies_ratings_df.withColumn("genres", split(col("genres"), "\|"))

# Ordena o DataFrame 'movies_df' pela coluna 'movieId' em ordem crescente
movies_ratings_df = movies_ratings_df.orderBy("average_rating", ascending=False)

# Removendo as aspas duplas dos valores da coluna 'title'
movies_ratings_df = movies_ratings_df.withColumn("title", regexp_replace(col("title"), '^"|"$', ''))

# Mostrar o DataFrame resultante
movies_ratings_df.show()
