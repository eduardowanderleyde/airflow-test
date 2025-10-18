FROM apache/airflow:2.10.2

USER root

# Instalar Java e ferramentas necessárias
RUN apt-get update && \
    apt-get install -y curl openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

# Baixar e instalar Spark
RUN curl -fL https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    ln -s /opt/spark-3.4.0-bin-hadoop3 /opt/spark

# Configurar variáveis de ambiente do Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Baixar JARs S3A diretamente e empacotar na imagem
# Isso evita problemas de download em runtime via Maven
RUN mkdir -p /opt/airflow/jars && \
    curl -fL -o /opt/airflow/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fL -o /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    chmod 644 /opt/airflow/jars/*.jar

USER airflow

# Instalar dependências Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

