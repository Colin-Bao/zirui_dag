
# pyarrow 读取数据
def pyarrow_read_parquet(parquet_path: str):
    import pyarrow.parquet as pq
    pq_table = pq.read_table(parquet_path)
    print(pq_table)


# pyspark 读取数据
def pyspark_read_parquet(parquet_path: str):
    # 方法一pyspark.pandas
    import pyspark.pandas as ps
    read_by_pandas = ps.read_parquet(parquet_path)
    print(read_by_pandas)
    # 方法二 SparkSession
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    read_by_ss = spark.read.parquet(parquet_path)
    # 方法三 SQLContext
    sc = spark.sparkContext
    from pyspark.sql import SQLContext
    sqlContext = SQLContext(sc)
    read_by_sc = sqlContext.read.parquet(parquet_path)
    print(read_by_sc)


pyarrow_read_parquet('20221012.parquet')
pyspark_read_parquet('20221012.parquet')
