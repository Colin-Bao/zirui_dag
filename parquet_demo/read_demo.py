# pyarrow 读取数据
def pyarrow_read_parquet(parquet_path: str):
    import pyarrow.parquet as pq
    pq_table = pq.read_table(parquet_path)
    print(pq_table)


pyarrow_read_parquet('parquet_demo/ASHARECASHFLOW.parquet')
