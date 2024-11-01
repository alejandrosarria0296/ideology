from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, split, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create Intervention DataFrame") \
    .getOrCreate()

def create_intervention_df(df):
    df = df.withColumn("interventions", split(col("interventions"), ","))

    # Explode the 'interventions' array into individual rows
    exploded_df = df.withColumn("intervention", explode(col("interventions")))
    
    # Add a unique ID for each intervention
    intervention_df = exploded_df.withColumn("id", monotonically_increasing_id())
    
    return intervention_df.select("session", "id", "intervention")

if __name__ == "__main__":
    # Define schema for CSV file
    schema = StructType([
        StructField("session", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("interventions", StringType(), True)  # Read as StringType initially
    ])
    
    # Load the CSV file
    original_df = r"/home/asarria/ideology/data/session_texts.csv"
    print("Starting Spark job...")
    try:
        df = spark.read.csv(original_df, schema=schema, header=True)
        print("CSV file read successfully")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
    
    # Process interventions
    intervention_df = create_intervention_df(df)
    
    # Save the result to CSV
    intervention_df.coalesce(1).write.csv(r"data/interventions.csv", header=True)
    print("Number of rows in intervention DataFrame:", intervention_df.count())
    # Stop the Spark session
    spark.stop()