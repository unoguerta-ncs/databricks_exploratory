from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()

    # Input
    df = spark.read.option("header", True).csv("/Volumes/workspace/default/raw/input.csv")

    # Transform
    df = df.filter(df["value"].isNotNull())

    # Output
    df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/processed/output")

if __name__ == "__main__":
    main()

