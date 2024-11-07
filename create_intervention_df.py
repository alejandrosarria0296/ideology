from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create Intervention DataFrame") \
    .config("spark.sql.files.maxPartitionBytes", "256MB") \
    .config("spark.sql.files.maxRecordLength", "134217728") \
    .getOrCreate()

def create_intervention_df(df):
    # Process the 'interventions' column by removing outer brackets and splitting by inner pattern
    df = df.withColumn("interventions", F.regexp_replace("interventions", r"^\[|\]$", "")) \
           .withColumn("interventions", F.split(F.col("interventions"), r"\], \["))

    # Now explode the 'interventions' array to create individual rows
    exploded_df = df.withColumn("intervention", F.explode(F.col("interventions")))

    # Add a unique ID for each intervention
    intervention_df = exploded_df.withColumn("int_id", F.monotonically_increasing_id())

    # Select and rename columns: 'id' as 'session', and use 'intervention' for the exploded values
    return intervention_df.select(F.col("id").alias("session"), "int_id", "intervention")

if __name__ == "__main__":
    # Define schema for CSV file
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("chamber", StringType(), True),
        StructField("type", StringType(), True),
        StructField("raw_text", StringType(), True),
        StructField("clean_text", StringType(), True),
        StructField("intervention_pairs", StringType(), True),
        StructField("interventions", StringType(), True)
    ])
    
    # Load the CSV file
    original_df = r"/home/asarria/ideology/data/session_texts.csv"
    print("Starting Spark job...")
    try:
        df = spark.read.csv(original_df, schema=schema, header=True, escape='"', maxCharsPerColumn=8192000)
        print("CSV file read successfully")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
    
    # Process interventions
    intervention_df = create_intervention_df(df)
    
    # Save the result to CSV
    intervention_df.write.csv(r"data/interventions", header=True, mode="overwrite")
    print("Number of rows in intervention DataFrame:", intervention_df.count())
    
    # Stop the Spark session
    spark.stop()
