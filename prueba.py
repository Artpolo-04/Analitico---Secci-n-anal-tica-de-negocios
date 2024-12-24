from pyspark.sql import SparkSession

def simple_example():
    spark = SparkSession.builder.appName("SimpleExample").getOrCreate()
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Nombre", "Edad"]
    df = spark.createDataFrame(data, columns)
    df.show()
    spark.stop()

simple_example()