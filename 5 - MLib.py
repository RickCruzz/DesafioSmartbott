#introdution to Mlib

#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("mLib").getOrCreate()

#%%
#ReadData
df_pyspark = spark.read.csv('./inputs/Mlib.csv', header=True, inferSchema=True)
df_pyspark.show()

#%%
df_pyspark.columns
df_pyspark.printSchema()

#%%
from pyspark.ml.feature import VectorAssembler
featureassembler=VectorAssembler(inputCols=["age","Experience"], outputCol="Independent Features")


#%%
output=featureassembler.transform(df_pyspark)
output.show()

#%%
final_data = output.select("Independent Features","Salary")
final_data.show()


#%%
from pyspark.ml.regression import LinearRegression
#train test split
train_data, test_data = final_data.randomSplit([0.75,0.25])
regressor = LinearRegression(featuresCol='Independent Features', labelCol='Salary')
regressor = regressor.fit(train_data)

#%%
#Coeficientes
regressor.coefficients

#%%
#intercepts
regressor.intercept

#%%
#predictions
pred_results=regressor.evaluate(test_data)
#%%
pred_results.predictions.show()

#%%
