import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CountRows").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

DATA_FOLDER = os.getenv("DATA_FOLDER")

lines = spark.read.csv(f"{DATA_FOLDER}/monroe-county-crash.csv", header=True)
count = lines.count()

print(f"Number of rows: {count}")

with open(f"{DATA_FOLDER}/count.txt", "w") as file:
    file.write(str(count))

spark.stop()

