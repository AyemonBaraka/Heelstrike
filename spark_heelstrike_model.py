from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType,StructField,LongType, StringType,DoubleType,TimestampType
from pyspark.ml.feature import OneHotEncoder, MinMaxScaler, StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Heelstrike_Model") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType( \
                     [StructField("Time", DoubleType(),True), \
                      StructField("Gyro X", DoubleType(), True), \
                      StructField("Gyro Y", DoubleType(), True), \
                      StructField('Gyro Z', DoubleType(), True), \
                      StructField("Accel X", DoubleType(), True), \
                      StructField("Accel Y", DoubleType(), True), \
                      StructField("Accel Z", DoubleType(), True), \
                      StructField("label", LongType(), True),\
                        ])

# Load the CSV files into DataFrames
df_LF = spark.read.format('csv').option("header", "true").schema(schema).load("/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_70/LF_all_70.csv")
df_LH = spark.read.format('csv').option("header", "true").schema(schema).load("/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_70/LH_all_70.csv")
df_LS = spark.read.format('csv').option("header", "true").schema(schema).load("/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_70/LS_all_70.csv")
df_RF = spark.read.format('csv').option("header", "true").schema(schema).load("/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_70/RF_all_70.csv")
df_RH = spark.read.format('csv').option("header", "true").schema(schema).load("/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_70/RH_all_70.csv")
df_RS = spark.read.format('csv').option("header", "true").schema(schema).load("/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_70/RS_all_70.csv")
df_SA = spark.read.format('csv').option("header", "true").schema(schema).load("/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_70/SA_all_70.csv")

df_LF = df_LF.withColumnRenamed("Gyro X","LF_Gyro X").withColumnRenamed("Gyro Y","LF_Gyro Y").withColumnRenamed("Gyro Z","LF_Gyro Z")\
    .withColumnRenamed("Accel X","LF_Accel X").withColumnRenamed("Accel Y","LF_Accel Y").withColumnRenamed("Accel Z","LF_Accel Z")

df_LH = df_LH.withColumnRenamed("Gyro X","LH_Gyro X").withColumnRenamed("Gyro Y","LH_Gyro Y").withColumnRenamed("Gyro Z","LH_Gyro Z")\
    .withColumnRenamed("Accel X","LH_Accel X").withColumnRenamed("Accel Y","LH_Accel Y").withColumnRenamed("Accel Z","LH_Accel Z")

df_LS = df_LS.withColumnRenamed("Gyro X","LS_Gyro X").withColumnRenamed("Gyro Y","LS_Gyro Y").withColumnRenamed("Gyro Z","LS_Gyro Z")\
    .withColumnRenamed("Accel X","LS_Accel X").withColumnRenamed("Accel Y","LS_Accel Y").withColumnRenamed("Accel Z","LS_Accel Z")

df_RF = df_RF.withColumnRenamed("Gyro X","RF_Gyro X").withColumnRenamed("Gyro Y","RF_Gyro Y").withColumnRenamed("Gyro Z","RF_Gyro Z")\
    .withColumnRenamed("Accel X","RF_Accel X").withColumnRenamed("Accel Y","RF_Accel Y").withColumnRenamed("Accel Z","RF_Accel Z")

df_RH = df_RH.withColumnRenamed("Gyro X","RH_Gyro X").withColumnRenamed("Gyro Y","RH_Gyro Y").withColumnRenamed("Gyro Z","RH_Gyro Z")\
    .withColumnRenamed("Accel X","RH_Accel X").withColumnRenamed("Accel Y","RH_Accel Y").withColumnRenamed("Accel Z","RH_Accel Z")

df_RS = df_RS.withColumnRenamed("Gyro X","RS_Gyro X").withColumnRenamed("Gyro Y","RS_Gyro Y").withColumnRenamed("Gyro Z","RS_Gyro Z")\
    .withColumnRenamed("Accel X","RS_Accel X").withColumnRenamed("Accel Y","RS_Accel Y").withColumnRenamed("Accel Z","RS_Accel Z")

df_SA = df_SA.withColumnRenamed("Gyro X","SA_Gyro X").withColumnRenamed("Gyro Y","SA_Gyro Y").withColumnRenamed("Gyro Z","SA_Gyro Z")\
    .withColumnRenamed("Accel X","SA_Accel X").withColumnRenamed("Accel Y","SA_Accel Y").withColumnRenamed("Accel Z","SA_Accel Z")


df_LF.printSchema()
df_LH.printSchema()
df_LS.printSchema()
df_RF.printSchema()
df_RH.printSchema()
df_RS.printSchema()
df_SA.printSchema()
#df.show()


dataframes = [df_LF, df_LH, df_LS,df_RF,df_RH, df_RS,df_SA]

# Perform joins based on 'time' and 'label'
joined_df = dataframes[0]
for df in dataframes[1:]:
    join_cols = ['time', 'label']
    joined_df = joined_df.join(df, on=join_cols, how="inner")


# Show the joined DataFrame
joined_df.printSchema()
#joined_df.show()

trainDF, testDF = joined_df.randomSplit([0.70, 0.30], seed=123)

feature_cols = joined_df.columns
feature_cols.remove('label')



assembler1 = VectorAssembler(inputCols=feature_cols, outputCol="features_scaled1")
scaler = MinMaxScaler(inputCol="features_scaled1", outputCol="features_scaled2")
assembler2 = VectorAssembler(inputCols=['features_scaled2'], outputCol="features")

# Create a RandomForestClassifier
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=200)


# Create stages list
myStages = [assembler1, scaler, assembler2, rf]

# Set up the pipeline
pipeline = Pipeline(stages= myStages)

# We fit the model using the training data.
pModel = pipeline.fit(trainDF)# We transform the data.
trainingPred = pModel.transform(testDF)# # We select the actual label, probability and predictions
trainingPred.select('label','probability','prediction').show()

# Evaluate the model's performance
evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
accuracy = evaluator.evaluate(trainingPred)
print("Accuracy: ", accuracy)

trainingPred.crosstab('label','prediction').show()


# Save the trained model
model_save_path = "/home/ayemon/KafkaProjects/kafkaspark07_90/model_heelstrike"
pModel.write().overwrite().save(model_save_path)



"""
trainDF, testDF = df.randomSplit([0.75, 0.25], seed=42)

feature_cols = df.columns
feature_cols.remove('label')
lr = LogisticRegression(maxIter=10, regParam= 0.01)

# We create a one hot encoder.
#ohe = OneHotEncoder(inputCols = ['sex', 'cp', 'fbs', 'restecg', 'slp', 'exng', 'caa', 'thall'], outputCols=['sex_ohe', 'cp_ohe', 'fbs_ohe', 'restecg_ohe', 'slp_ohe', 'exng_ohe', 'caa_ohe', 'thall_ohe'])
# Input list for scaling
inputs = ['age','trtbps','chol','thalachh','oldpeak']

# We scale our inputs
assembler1 = VectorAssembler(inputCols=inputs, outputCol="features_scaled1")
scaler = MinMaxScaler(inputCol="features_scaled1", outputCol="features_scaled")
# We create a second assembler for the encoded columns.
#assembler2 = VectorAssembler(inputCols=['sex_ohe', 'cp_ohe', 'fbs_ohe', 'restecg_ohe', 'slp_ohe', 'exng_ohe', 'caa_ohe', 'thall_ohe','features_scaled'], outputCol="features")
assembler2 = VectorAssembler(inputCols=['features_scaled'], outputCol="features")

# Create stages list
myStages = [assembler1, scaler, assembler2,lr]

# Set up the pipeline
pipeline = Pipeline(stages= myStages)

# We fit the model using the training data.
pModel = pipeline.fit(trainDF)# We transform the data.
trainingPred = pModel.transform(testDF)# # We select the actual label, probability and predictions
trainingPred.select('label','probability','prediction').show()

# Evaluate the model's performance
evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
accuracy = evaluator.evaluate(trainingPred)
print("Accuracy: ", accuracy)

trainingPred.crosstab('label','prediction').show()

# Save the trained model
model_save_path = "/home/ayemon/KafkaProjects/kafkaspark07_90/model"
pModel.write().overwrite().save(model_save_path)

"""
