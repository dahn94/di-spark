import pyspark
from pandas.core.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import *
from pyspark.sql.types import StructField, LongType
from pyspark.sql import types
from functools import reduce
from operator import mul
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from delta import *
from delta.tables import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.load("data_entrada.csv", format="csv", sep=",", inferSchema="true", header="true")

df = df.drop('dataInicial', 'percentual', 'taxa', 'valorBase', 'valorCalculado')
df = df.withColumnRenamed("dataFinal", "data")
df = df.where("fator>1.00000000")

schema = df.schema.add(StructField("id", LongType()))
rdd = df.rdd.zipWithIndex()


def flat(l):
    for k in l:
        if not isinstance(k, (list, tuple)):
            yield k
        else:
            yield from flat(k)


rdd = rdd.map(lambda x: list(flat(x)))
df = spark.createDataFrame(rdd, schema)

window = Window.orderBy('id')
mul_udf = F.udf(lambda x: reduce(mul, x), types.DoubleType())
df = df.withColumn('fator_acumulado', mul_udf(F.collect_list(F.col('fator')).over(window)))

df = df.withColumn('fator_acumulado', df.fator_acumulado.cast("float"))


def get_taxaAcumulada(fator_acumulado):
    taxa_acumulada = (fator_acumulado - 1) * 100

    return taxa_acumulada


udf_func = udf(get_taxaAcumulada, FloatType())
df = df.withColumn("taxa_acumulada", udf_func(df.fator_acumulado))
df = df.drop('id')

df.write.format("delta").save("saida-delta-table")

read_df = spark.read.format("delta").load("saida-delta-table")
