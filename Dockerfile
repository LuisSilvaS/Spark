# Use a imagem oficial do Spark
FROM bitnami/spark:latest

WORKDIR /app

RUN mkdir -p output

# Execute o script PySpark quando o contêiner iniciar
CMD ["spark-submit", "/app/app.py"]