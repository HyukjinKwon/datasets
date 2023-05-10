import pyspark


def test_this_is_fine():
    spark = (
        pyspark.sql.SparkSession.builder.config("spark.python.worker.faulthandler.enabled", "true")
        .master("local[*]")
        .appName("pyspark")
        .getOrCreate()
    )
    data = [
        ("0", 0, 0.0),
        ("1", 1, 1.0),
        ("2", 2, 2.0),
        ("3", 3, 3.0),
    ]
    df = spark.createDataFrame(data, "col_1: string, col_2: int, col_3: float")

    def f(it):
        import sys

        raise Exception(sys.modules)

        for batch in it:
            yield batch

    df.mapInArrow(f, df.schema).collect()


def ff(it):
    import datasets

    datasets
    for batch in it:
        yield batch


def test_crash_from_map_in_arrow_group_by():
    spark = (
        pyspark.sql.SparkSession.builder.config("spark.python.worker.faulthandler.enabled", "true")
        .master("local[*]")
        .appName("pyspark")
        .getOrCreate()
    )
    data = [
        ("0", 0, 0.0),
        ("1", 1, 1.0),
        ("2", 2, 2.0),
        ("3", 3, 3.0),
    ]
    df = spark.createDataFrame(data, "col_1: string, col_2: int, col_3: float")

    df.mapInArrow(ff, df.schema).orderBy("col_1").collect()


def test_crash_from_map_in_arrow_order_by():
    spark = (
        pyspark.sql.SparkSession.builder.config("spark.python.worker.faulthandler.enabled", "true")
        .master("local[*]")
        .appName("pyspark")
        .getOrCreate()
    )
    data = [
        ("0", 0, 0.0),
        ("1", 1, 1.0),
        ("2", 2, 2.0),
        ("3", 3, 3.0),
    ]
    df = spark.createDataFrame(data, "col_1: string, col_2: int, col_3: float")

    def f(it):
        for batch in it:
            yield batch

    df.mapInArrow(f, df.schema).groupBy("col_1").count().collect()


def fff(it):
    import importlib

    mod = importlib.import_module("datasets.arrow_writer")

    for batch in it:
        mod.ArrowWriter(path="dummy.txt")
        yield batch


def test_crash_from_map_in_arrow_arrow_writer():
    spark = (
        pyspark.sql.SparkSession.builder.config("spark.python.worker.faulthandler.enabled", "true")
        .master("local[*]")
        .appName("pyspark")
        .getOrCreate()
    )
    data = [
        ("0", 0, 0.0),
        ("1", 1, 1.0),
        ("2", 2, 2.0),
        ("3", 3, 3.0),
    ]
    df = spark.createDataFrame(data, "col_1: string, col_2: int, col_3: float")

    df.mapInArrow(fff, df.schema).collect()


def test_crash_from_order_by_partition():
    spark = (
        pyspark.sql.SparkSession.builder.config("spark.python.worker.faulthandler.enabled", "true")
        .master("local[*]")
        .appName("pyspark")
        .getOrCreate()
    )
    data = [
        ("0", 0, 0.0),
        ("1", 1, 1.0),
        ("2", 2, 2.0),
        ("3", 3, 3.0),
    ]
    df = spark.createDataFrame(data, "col_1: string, col_2: int, col_3: float")

    df.orderBy(pyspark.sql.functions.spark_partition_id()).collect()
