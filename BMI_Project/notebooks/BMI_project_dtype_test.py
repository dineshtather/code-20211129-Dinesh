# Databricks notebook source
from typing import List, Dict, Any, Callable
from pyspark.sql import SparkSession, DataFrame, Column
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
  BMI_Input_Test = spark.read.json(f"abfss://Container_File_Path/BMI_Input_Test.json",multiLine=True)
  #print(f"Check the data type in the input file: {BMI_Input_Test.dtypes}")
  for col in BMI_Input_Test.dtypes:
      #print(col[0],",",col[1])
      if col[0] == 'Gender':
        if col[1] != 'string':
          print(col[0],",",col[1])
          print("Check gender data type as it looks other then string")
      elif col[0] == 'HeightCm': 
        if col[1] != 'bigint':
          print(col[0],",",col[1])
          print("Check the Height data type as it looks other than a number")
      elif col[0] == 'WeightKg': 
        if col[1] != 'bigint':
          print(col[0],",",col[1])
          print("Check the Weight data type as it looks other than a number")          
        