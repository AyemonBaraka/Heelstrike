from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType
from pyspark.sql.functions import col, from_json
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# Load the saved logistic regression model
model_save_path = "/home/ayemon/KafkaProjects/kafkaspark07_90/model_heelstrike" # Replace with the path where you saved the model
loaded_lrModel = PipelineModel.load(model_save_path)

# Kafka broker address
kafka_broker = "10.18.17.153:9092"

# Kafka topics to send the data
topic_LF = "Heelstrike_LF"
topic_LH = "Heelstrike_LH"
topic_LS = "Heelstrike_LS"
topic_RF = "Heelstrike_RF"
topic_RH = "Heelstrike_RH"
topic_RS = "Heelstrike_RS"
topic_SA = "Heelstrike_SA"

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType( \
                     [StructField("Time", StringType(),True), \
                      StructField("Gyro X", StringType(), True), \
                      StructField("Gyro Y", StringType(), True), \
                      StructField('Gyro Z', StringType(), True), \
                      StructField("Accel X", StringType(), True), \
                      StructField("Accel Y", StringType(), True), \
                      StructField("Accel Z", StringType(), True), \
                      StructField("label", StringType(), True),\
                        ])
# Define the Kafka consumer settings
kafka_df_LF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_LF) \
    .load()

kafka_df_LH = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_LH) \
    .load()

kafka_df_LS = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_LS) \
    .load()

kafka_df_RF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_RF) \
    .load()

kafka_df_RH = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_RH) \
    .load()

kafka_df_RS = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_RS) \
    .load()

kafka_df_SA = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_SA) \
    .load()

# Convert the Kafka value to a DataFrame column
kafka_df_LF = kafka_df_LF.selectExpr("CAST(value AS STRING)")
kafka_df_LH = kafka_df_LH.selectExpr("CAST(value AS STRING)")
kafka_df_LS = kafka_df_LS.selectExpr("CAST(value AS STRING)")
kafka_df_RF = kafka_df_RF.selectExpr("CAST(value AS STRING)")
kafka_df_RH = kafka_df_RH.selectExpr("CAST(value AS STRING)")
kafka_df_RS = kafka_df_RS.selectExpr("CAST(value AS STRING)")
kafka_df_SA = kafka_df_SA.selectExpr("CAST(value AS STRING)")

# Apply the schema to the data
kafka_df_LF = kafka_df_LF.select(from_json("value", schema).alias("data")).select("data.*")
kafka_df_LH = kafka_df_LH.select(from_json("value", schema).alias("data")).select("data.*")
kafka_df_LS = kafka_df_LS.select(from_json("value", schema).alias("data")).select("data.*")
kafka_df_RF = kafka_df_RF.select(from_json("value", schema).alias("data")).select("data.*")
kafka_df_RH = kafka_df_RH.select(from_json("value", schema).alias("data")).select("data.*")
kafka_df_RS = kafka_df_RS.select(from_json("value", schema).alias("data")).select("data.*")
kafka_df_SA = kafka_df_SA.select(from_json("value", schema).alias("data")).select("data.*")


# Change the data types of columns to DoubleType
kafka_df_LF = kafka_df_LF.withColumn("Time",col("Time").cast('double'))\
    .withColumn("Gyro X",col("Gyro X").cast('double'))\
    .withColumn("Gyro Y",col("Gyro Y").cast('double'))\
    .withColumn("Gyro Z",col("Gyro Z").cast('double'))\
    .withColumn("Accel X",col("Accel X").cast('double'))\
    .withColumn("Accel Y", col("Accel Y").cast('double'))\
    .withColumn("Accel Z", col("Accel Z").cast('double'))\
    .withColumn("label", col("label").cast('long'))

kafka_df_LH = kafka_df_LH.withColumn("Time",col("Time").cast('double'))\
    .withColumn("Gyro X",col("Gyro X").cast('double'))\
    .withColumn("Gyro Y",col("Gyro Y").cast('double'))\
    .withColumn("Gyro Z",col("Gyro Z").cast('double'))\
    .withColumn("Accel X",col("Accel X").cast('double'))\
    .withColumn("Accel Y", col("Accel Y").cast('double'))\
    .withColumn("Accel Z", col("Accel Z").cast('double'))\
    .withColumn("label", col("label").cast('long'))

kafka_df_LS = kafka_df_LS.withColumn("Time",col("Time").cast('double'))\
    .withColumn("Gyro X",col("Gyro X").cast('double'))\
    .withColumn("Gyro Y",col("Gyro Y").cast('double'))\
    .withColumn("Gyro Z",col("Gyro Z").cast('double'))\
    .withColumn("Accel X",col("Accel X").cast('double'))\
    .withColumn("Accel Y", col("Accel Y").cast('double'))\
    .withColumn("Accel Z", col("Accel Z").cast('double'))\
    .withColumn("label", col("label").cast('long'))

kafka_df_RF = kafka_df_RF.withColumn("Time",col("Time").cast('double'))\
    .withColumn("Gyro X",col("Gyro X").cast('double'))\
    .withColumn("Gyro Y",col("Gyro Y").cast('double'))\
    .withColumn("Gyro Z",col("Gyro Z").cast('double'))\
    .withColumn("Accel X",col("Accel X").cast('double'))\
    .withColumn("Accel Y", col("Accel Y").cast('double'))\
    .withColumn("Accel Z", col("Accel Z").cast('double'))\
    .withColumn("label", col("label").cast('long'))

kafka_df_RH = kafka_df_RH.withColumn("Time",col("Time").cast('double'))\
    .withColumn("Gyro X",col("Gyro X").cast('double'))\
    .withColumn("Gyro Y",col("Gyro Y").cast('double'))\
    .withColumn("Gyro Z",col("Gyro Z").cast('double'))\
    .withColumn("Accel X",col("Accel X").cast('double'))\
    .withColumn("Accel Y", col("Accel Y").cast('double'))\
    .withColumn("Accel Z", col("Accel Z").cast('double'))\
    .withColumn("label", col("label").cast('long'))

kafka_df_RS = kafka_df_RS.withColumn("Time",col("Time").cast('double'))\
    .withColumn("Gyro X",col("Gyro X").cast('double'))\
    .withColumn("Gyro Y",col("Gyro Y").cast('double'))\
    .withColumn("Gyro Z",col("Gyro Z").cast('double'))\
    .withColumn("Accel X",col("Accel X").cast('double'))\
    .withColumn("Accel Y", col("Accel Y").cast('double'))\
    .withColumn("Accel Z", col("Accel Z").cast('double'))\
    .withColumn("label", col("label").cast('long'))

kafka_df_SA = kafka_df_SA.withColumn("Time",col("Time").cast('double'))\
    .withColumn("Gyro X",col("Gyro X").cast('double'))\
    .withColumn("Gyro Y",col("Gyro Y").cast('double'))\
    .withColumn("Gyro Z",col("Gyro Z").cast('double'))\
    .withColumn("Accel X",col("Accel X").cast('double'))\
    .withColumn("Accel Y", col("Accel Y").cast('double'))\
    .withColumn("Accel Z", col("Accel Z").cast('double'))\
    .withColumn("label", col("label").cast('long'))

#Rename the columns
kafka_df_LF = kafka_df_LF.withColumnRenamed("Gyro X","LF_Gyro X").withColumnRenamed("Gyro Y","LF_Gyro Y").withColumnRenamed("Gyro Z","LF_Gyro Z")\
    .withColumnRenamed("Accel X","LF_Accel X").withColumnRenamed("Accel Y","LF_Accel Y").withColumnRenamed("Accel Z","LF_Accel Z")

kafka_df_LH = kafka_df_LH.withColumnRenamed("Gyro X","LH_Gyro X").withColumnRenamed("Gyro Y","LH_Gyro Y").withColumnRenamed("Gyro Z","LH_Gyro Z")\
    .withColumnRenamed("Accel X","LH_Accel X").withColumnRenamed("Accel Y","LH_Accel Y").withColumnRenamed("Accel Z","LH_Accel Z")

kafka_df_LS = kafka_df_LS.withColumnRenamed("Gyro X","LS_Gyro X").withColumnRenamed("Gyro Y","LS_Gyro Y").withColumnRenamed("Gyro Z","LS_Gyro Z")\
    .withColumnRenamed("Accel X","LS_Accel X").withColumnRenamed("Accel Y","LS_Accel Y").withColumnRenamed("Accel Z","LS_Accel Z")

kafka_df_RF = kafka_df_RF.withColumnRenamed("Gyro X","RF_Gyro X").withColumnRenamed("Gyro Y","RF_Gyro Y").withColumnRenamed("Gyro Z","RF_Gyro Z")\
    .withColumnRenamed("Accel X","RF_Accel X").withColumnRenamed("Accel Y","RF_Accel Y").withColumnRenamed("Accel Z","RF_Accel Z")

kafka_df_RH = kafka_df_RH.withColumnRenamed("Gyro X","RH_Gyro X").withColumnRenamed("Gyro Y","RH_Gyro Y").withColumnRenamed("Gyro Z","RH_Gyro Z")\
    .withColumnRenamed("Accel X","RH_Accel X").withColumnRenamed("Accel Y","RH_Accel Y").withColumnRenamed("Accel Z","RH_Accel Z")

kafka_df_RS = kafka_df_RS.withColumnRenamed("Gyro X","RS_Gyro X").withColumnRenamed("Gyro Y","RS_Gyro Y").withColumnRenamed("Gyro Z","RS_Gyro Z")\
    .withColumnRenamed("Accel X","RS_Accel X").withColumnRenamed("Accel Y","RS_Accel Y").withColumnRenamed("Accel Z","RS_Accel Z")

kafka_df_SA = kafka_df_SA.withColumnRenamed("Gyro X","SA_Gyro X").withColumnRenamed("Gyro Y","SA_Gyro Y").withColumnRenamed("Gyro Z","SA_Gyro Z")\
    .withColumnRenamed("Accel X","SA_Accel X").withColumnRenamed("Accel Y","SA_Accel Y").withColumnRenamed("Accel Z","SA_Accel Z")

dataframes = [kafka_df_LF, kafka_df_LH, kafka_df_LS, kafka_df_RF, kafka_df_RH, kafka_df_RS, kafka_df_SA]

# Perform joins based on 'time' and 'label'
joined_df = dataframes[0]
for df in dataframes[1:]:
    join_cols = ['time', 'label']
    joined_df = joined_df.join(df, on=join_cols, how="inner")


# Show the joined DataFrame
joined_df.printSchema()
#joined_df.show()

"""joined_query = joined_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
"""
# Make predictions using the loaded logistic regression model
predictions = loaded_lrModel.transform(joined_df)

# Select the original columns and the prediction column
predictions = predictions.select('time','label','prediction')

# Start the streaming query to continuously read from Kafka and make predictions
query = predictions.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()