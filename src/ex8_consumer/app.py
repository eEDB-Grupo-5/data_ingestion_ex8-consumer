"""
This script sets up and runs a PySpark pipeline to process data streams.

The pipeline reads data from a Kafka topic named 'my-topic', joins it with
'empregados' and 'bancos' tables from a PostgreSQL database, and prints the
results to the console.

Key functionalities include:
- Setting up a Spark session with Kafka, Avro, and PostgreSQL connectors.
- Defining a streaming source from Kafka and batch sources from JDBC.
- Executing a SQL query to perform the join and aggregation.
- Sending the processed data to a console sink for output.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType,
)
from ex8_consumer import settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _get_schema_str() -> str:
    """Reads the Avro schema and returns it as a string."""
    try:
        with open(settings.SCHEMA_PATH, "r") as f:
            schema_str = f.read()
        return schema_str
    except FileNotFoundError:
        logger.error(f"Schema file not found at: {settings.SCHEMA_PATH}")
        raise
    except Exception as e:
        logger.error(f"Failed to read schema: {e}")
        raise


AVRO_SCHEMA_STR = _get_schema_str()

def main():
    # Set up the Spark session
    spark = (
        SparkSession.builder.appName("Ex8Consumer")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1,org.postgresql:postgresql:42.5.4,org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")

    # JDBC sources
    jdbc_url = f"jdbc:postgresql://{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.DB_NAME}"
    connection_properties = {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    # Create the table if it doesn't exist
    schema = StructType([
        StructField("ano", IntegerType(), True),
        StructField("trimestre", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("tipo", StringType(), True),
        StructField("cnpj", StringType(), True),
        StructField("instituicao_financeira", StringType(), True),
        StructField("indice", DecimalType(), True),
        StructField("qtd_reclamacoes_reguladas_procedentes", IntegerType(), True),
        StructField("qtd_reclamacoes_reguladas_outras", IntegerType(), True),
        StructField("qtd_reclamacoes_nao_reguladas", IntegerType(), True),
        StructField("qtd_total_clientes_ccs_e_scr", IntegerType(), True),
        StructField("qtd_clientes_ccs", IntegerType(), True),
        StructField("qtd_clientes_scr", IntegerType(), True),
        StructField("qtd_total_reclamacoes", IntegerType(), True)
    ])

    spark.createDataFrame([], schema).write.jdbc(
        url=jdbc_url,
        table="trusted.reclamacoes_cleaned",
        mode="ignore",
        properties=connection_properties
    )

    # Kafka source
    reclamacoes_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", "my-topic")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Deserialize Avro data
    reclamacoes_df = reclamacoes_stream_df.select(
        from_avro(col("value"), AVRO_SCHEMA_STR).alias("reclamacoes")
    ).select("reclamacoes.*")

    reclamacoes_df.createOrReplaceTempView("reclamacoes_stream")

    # JDBC sources
    jdbc_url = f"jdbc:postgresql://{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.DB_NAME}"
    connection_properties = {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    empregados_df = (
        spark.read.jdbc(
            url=jdbc_url,
            table="trusted.empregados",
            properties=connection_properties,
        )
    )
    empregados_df.createOrReplaceTempView("empregados")

    bancos_df = spark.read.jdbc(
        url=jdbc_url, table="trusted.bancos", properties=connection_properties
    )
    bancos_df.createOrReplaceTempView("bancos")

    reclamacoes_cleaned_df = spark.sql(r"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            trimestre,
            categoria,
            tipo,
            LPAD(TRIM(cnpj_if), 8, '0') AS cnpj,
            TRIM(REGEXP_REPLACE(UPPER(TRIM(instituicao_financeira)), '\s*(S\s*\.?\s*A\s*\.?((s*-\s*)?\s*CFI)?|LTDA\.?|- CFI - PRUDENCIAL|- CFI|- PRUDENCIAL|S\s*\.?\s*A\s*\.?\ - PRUDENCIAL|\(CONGLOMERADO\))$', '')) AS instituicao_financeira,
            CAST(REPLACE(REPLACE(TRIM(CASE WHEN indice != '' THEN indice END), '.', ''), ',', '.')  AS DECIMAL) AS indice,
            CAST(CASE WHEN quantidade_de_reclamacoes_reguladas_procedentes != '' THEN quantidade_de_reclamacoes_reguladas_procedentes END AS INTEGER) AS qtd_reclamacoes_reguladas_procedentes,
            CAST(CASE WHEN quantidade_de_reclamacoes_reguladas_outras != '' THEN quantidade_de_reclamacoes_reguladas_outras END AS INTEGER) AS qtd_reclamacoes_reguladas_outras,
            CAST(CASE WHEN quantidade_de_reclamacoes_nao_reguladas != '' THEN quantidade_de_reclamacoes_nao_reguladas END AS INTEGER) AS qtd_reclamacoes_nao_reguladas,
            CAST(CASE WHEN quantidade_total_de_clientes_ccs_e_scr != '' THEN quantidade_total_de_clientes_ccs_e_scr END AS INTEGER) AS qtd_total_clientes_ccs_e_scr,
            CAST(CASE WHEN quantidade_de_clientes_ccs != '' THEN quantidade_de_clientes_ccs END AS INTEGER) AS qtd_clientes_ccs,
            CAST(CASE WHEN quantidade_de_clientes_scr != '' THEN quantidade_de_clientes_scr END AS INTEGER) AS qtd_clientes_scr,
            CAST(CASE WHEN quantidade_total_de_reclamacoes != '' THEN quantidade_total_de_reclamacoes END AS INTEGER) AS qtd_total_reclamacoes
        FROM reclamacoes_stream
    """)

    reclamacoes_cleaned_df.createOrReplaceTempView("reclamacoes_cleaned_stream")

    # Write to PostgreSQL
    query_reclamacoes_to_postgres = (
        reclamacoes_cleaned_df.writeStream
        .foreachBatch(lambda df, epoch_id: df.write
            .jdbc(
                url=jdbc_url,
                table="trusted.reclamacoes_cleaned",
                mode="append",
                properties=connection_properties
            )
        )
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoint_reclamacoes")
        .start()
    )

    # SQL query
    query = r"""
        WITH reclamacoes_agg AS (
            SELECT
                ano,
                trimestre,
                categoria,
                tipo,
                cnpj,
                instituicao_financeira,
                AVG(indice) AS indice,
                SUM(qtd_reclamacoes_reguladas_procedentes) AS qtd_reclamacoes_reguladas_procedentes,
                SUM(qtd_reclamacoes_reguladas_outras) AS qtd_reclamacoes_reguladas_outras,
                SUM(qtd_reclamacoes_nao_reguladas) AS qtd_reclamacoes_nao_reguladas,
                SUM(qtd_total_clientes_ccs_e_scr) AS qtd_total_clientes_ccs_e_scr,
                SUM(qtd_clientes_ccs) AS qtd_clientes_ccs,
                SUM(qtd_clientes_scr) AS qtd_clientes_scr,
                SUM(qtd_total_reclamacoes) AS qtd_total_reclamacoes
            FROM reclamacoes_cleaned_stream
            GROUP BY ano,
                trimestre,
                categoria,
                tipo,
                cnpj,
                instituicao_financeira
        ),
        emp_bco_join AS (
            SELECT
                emp.employer_name,
                emp.reviews_count,
                emp.culture_count,
                emp.salaries_count,
                emp.benefits_count,
                emp.employerwebsite,
                emp.employerheadquarters,
                emp.employerfounded,
                emp.employerindustry,
                emp.employerrevenue,
                emp.url,
                emp.geral,
                emp.cultura_e_valores,
                emp.diversidade_e_inclusao,
                emp.qualidade_de_vida,
                emp.alta_lideranca,
                emp.remuneracao_e_beneficios,
                emp.oportunidades_de_carreira,
                emp.recomendam_para_outras_pessoas,
                emp.perspectiva_positiva_da_empresa,
                COALESCE(bco.segmento, emp.segmento) AS segmento,
                COALESCE(bco.cnpj, emp.cnpj) AS cnpj,
                COALESCE(bco.nome, emp.nome) AS nome
            FROM empregados AS emp
            LEFT JOIN bancos AS bco
                ON emp.cnpj = bco.cnpj OR emp.nome = bco.nome
        )
        SELECT
            rec.ano,
            rec.trimestre,
            rec.categoria,
            rec.tipo,
            rec.cnpj,
            rec.instituicao_financeira,
            rec.indice,
            rec.qtd_reclamacoes_reguladas_procedentes,
            rec.qtd_reclamacoes_reguladas_outras,
            rec.qtd_reclamacoes_nao_reguladas,
            rec.qtd_total_clientes_ccs_e_scr,
            rec.qtd_clientes_ccs,
            rec.qtd_clientes_scr,
            rec.qtd_total_reclamacoes,
            gdoor.employer_name,
            gdoor.reviews_count,
            gdoor.culture_count,
            gdoor.salaries_count,
            gdoor.benefits_count,
            gdoor.employerwebsite,
            gdoor.employerheadquarters,
            gdoor.employerfounded,
            gdoor.employerindustry,
            gdoor.employerrevenue,
            gdoor.url,
            gdoor.geral,
            gdoor.cultura_e_valores,
            gdoor.diversidade_e_inclusao,
            gdoor.qualidade_de_vida,
            gdoor.alta_lideranca,
            gdoor.remuneracao_e_beneficios,
            gdoor.oportunidades_de_carreira,
            gdoor.recomendam_para_outras_pessoas,
            gdoor.perspectiva_positiva_da_empresa,
            gdoor.segmento,
            gdoor.nome
        FROM reclamacoes_agg AS rec
        INNER JOIN emp_bco_join AS gdoor
            ON rec.cnpj = gdoor.cnpj OR rec.instituicao_financeira = gdoor.nome
    """

    result_df = spark.sql(query)

    # Configure S3 access for Hadoop (s3a)
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", settings.AWS_ENDPOINT_URL)
    hadoop_conf.set("fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    def foreach_batch_write(df, epoch_id):
        # try:
        #     # Write to Postgres (append)
        #     df.write.jdbc(
        #         url=jdbc_url,
        #         table="trusted.reclamacoes_joined",
        #         mode="append",
        #         properties=connection_properties,
        #     )
        # except Exception:
        #     df.limit(0).write.jdbc(
        #         url=jdbc_url,
        #         table="trusted.reclamacoes_joined",
        #         mode="ignore",
        #         properties=connection_properties,
        #     )
        #     df.write.jdbc(
        #         url=jdbc_url,
        #         table="trusted.reclamacoes_joined",
        #         mode="append",
        #         properties=connection_properties,
        #     )

        # Write parquet files to S3
        s3_path = f"s3a://{settings.S3_BUCKET_NAME}/reclamacoes_joined/epoch={epoch_id}"
        try:
            df.write.mode("append").parquet(s3_path)
        except Exception as e:
            logger.error(f"Failed to write parquet to S3 at {s3_path}: {e}")

    query_stream = (
        result_df.writeStream
        .outputMode("complete")
        .foreachBatch(foreach_batch_write)
        .option("checkpointLocation", "/tmp/checkpoint_result_df")
        .start()
    )

    query_stream.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
