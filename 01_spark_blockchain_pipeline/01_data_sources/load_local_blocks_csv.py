from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("load-local-blocks-csv")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    input_path = "data/raw/blocks.csv"
    output_path = "data/raw/local_blocks_parquet"

    print(f"=== Reading local CSV from {input_path} ===")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print("=== Raw local block sample ===")
    df.show(10, truncate=False)

    print(f"=== Saving local raw blocks to {output_path} ===")
    df.write.mode("overwrite").parquet(output_path)
    print("Save completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()