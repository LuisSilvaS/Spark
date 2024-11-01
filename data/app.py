from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
from datetime import datetime

# Crie uma sessão Spark
spark = SparkSession.builder.appName("DockerSparkExample").getOrCreate()

# Leia o arquivo CSV
df = spark.read.csv("input/data.csv", header=True, inferSchema=True)

# Mostre o DataFrame original
df.show()

# Filtre pessoas com idade maior que 30
df_filtered = df.filter(df['age'] > 30)

# Mostre o DataFrame filtrado
df_filtered.show()

# Adicione uma nova coluna calculada
df_with_age_category = df_filtered.withColumn(
    "age_category", 
    when(col("age").between(31, 40), "31-40").otherwise("41+")
)

# Mostre o DataFrame com a nova coluna
df_with_age_category.show()

# Calcule a idade média
age_avg = df_with_age_category.agg(avg("age").alias("average_age")).collect()[0]["average_age"]
print(f"Average age: {age_avg}")

# Agrupe por categoria de idade e conte o número de pessoas em cada categoria
df_grouped = df_with_age_category.groupBy("age_category").count()

# Mostre o DataFrame agrupado
df_grouped.show()

# Encontre a pessoa mais velha
oldest_person = df_with_age_category.orderBy(col("age").desc()).first()
print(f"Oldest person: {oldest_person['name']} with age {oldest_person['age']}")

# Obtenha o timestamp atual
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# Crie o nome do arquivo com o timestamp
output_path_filtered = f"output/filtered_data_{timestamp}.csv"
output_path_grouped = f"output/grouped_data_{timestamp}.csv"

# Salve os resultados em novos arquivos CSV com o timestamp no nome
df_with_age_category.write.csv(output_path_filtered, header=True)
df_grouped.write.csv(output_path_grouped, header=True)

# Pare a sessão Spark
spark.stop()