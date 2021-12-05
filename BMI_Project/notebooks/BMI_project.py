# Databricks notebook source
from pyspark.sql.streaming import StreamingQuery, DataStreamReader, DataStreamWriter
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaMergeBuilder, DeltaTable
from typing import List, Dict, Any, Callable
from pyspark.sql import SparkSession, DataFrame, Column
from dataclasses import dataclass, field
import pyspark.sql.functions as F
import functools as ft
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType

# COMMAND ----------

#<<I used here azure blob container account details for validating a storage account>>

# COMMAND ----------

if __name__ == "__main__":
  #This is the input BMI Dataframe we will use for implementing the BMI App
  BMI_Input = spark.read.json(f"abfss://Container_File_Path/BMI_Input.json",multiLine=True)
  BMI_Input = BMI_Input.withColumn("BMI",(F.col("WeightKg")/F.col("HeightCm")) * 100)
  BMI_Input.show()

  #This is the Categorisation and risk included in the dataframe
  BMI_Cat_Risk = BMI_Input.withColumn("BMI_Cat", F.when((F.col("BMI") > 0) & (F.col("BMI") <= 18.4), F.lit("Underweight"))
                                                  .when((F.col("BMI") > 18.5) & (F.col("BMI") <= 24.9), F.lit("Normal weight"))
                                                  .when((F.col("BMI") > 25) & (F.col("BMI") <= 29.9), F.lit("Overweight"))
                                                  .when((F.col("BMI") > 30) & (F.col("BMI") <= 34.9), F.lit("Moderately obese"))
                                                  .when((F.col("BMI") > 35) & (F.col("BMI") <= 39.9), F.lit("Severely obese"))
                                                  .when((F.col("BMI") > 40), F.lit("Very severely obese"))) \
                          .withColumn("BMI_Range", F.when((F.col("BMI") > 0) & (F.col("BMI") <= 18.4), F.lit("18.4 and below"))
                                                    .when((F.col("BMI") > 18.5) & (F.col("BMI") <= 24.9), F.lit("18.5 - 24.9"))
                                                    .when((F.col("BMI") > 25) & (F.col("BMI") <= 29.9), F.lit("25 - 29.9"))
                                                    .when((F.col("BMI") > 30) & (F.col("BMI") <= 34.9), F.lit("30 - 34.9"))
                                                    .when((F.col("BMI") > 35) & (F.col("BMI") <= 39.9), F.lit("35 - 39.9"))
                                                    .when((F.col("BMI") > 40), F.lit("40 and above"))) \
                          .withColumn("BMI_Risk", F.when((F.col("BMI") > 0) & (F.col("BMI") <= 18.4), F.lit("Malnutrition risk"))
                                                   .when((F.col("BMI") > 18.5) & (F.col("BMI") <= 24.9), F.lit("Low risk"))
                                                   .when((F.col("BMI") > 25) & (F.col("BMI") <= 29.9), F.lit("Enhanced risk"))
                                                   .when((F.col("BMI") > 30) & (F.col("BMI") <= 34.9), F.lit("Medium risk"))
                                                   .when((F.col("BMI") > 35) & (F.col("BMI") <= 39.9), F.lit("High risk"))
                                                   .when((F.col("BMI") > 40), F.lit("Very high risk")))
  BMI_Cat_Risk.show()
  #Total number of overweight people
  BMI_Overweight = BMI_Cat_Risk.filter(F.col("BMI_Cat") == "Overweight").count()
  print(f"Total overweight people = {BMI_Overweight}")
  #Write this back to another JSON file
  BMI_Cat_Risk.toJSON(f"abfss://Container_File_Path/BMI_Output1.json")