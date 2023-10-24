from pyspark.sql import SparkSession
import logging
from time import gmtime, strftime
from  pyspark.sql.functions import input_file_name
from google.cloud import bigquery
from google.cloud import storage
import os
import sys
from pyspark.sql.functions import *
from uuid import uuid4
import numpy as np
import pandas as pd
import openpyxl
import pyxlsb
import glob

logging.basicConfig(level=logging.INFO)


class Remesas:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process
		os.system('mkdir remesas; mkdir remesas_csv')
		get_files = "gsutil -m cp {0} ./remesas/".format(source)
		os.system(get_files)

	def convert_excel_csv(self):
		for file in [doc for doc in os.listdir("./remesas/")]:
			df= pd.read_excel('./remesas/'+file)
			df.to_csv("./remesas_csv/"+file,index=False,header=True)
			os.system("hdfs dfs -put ./remesas_csv/ /")
			logging.info("success to convert_excel_to_csv")

	def convert_remesas_to_df(self):
		self.convert_excel_csv()
		df = self.spark.read.options(header='True',delimiter=',',encoding='ISO-8859-1').csv("hdfs:///remesas_csv/")
		df.show(truncate=False)
		self.df_remesas = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE", "NAME_FILE","ID_ROW", *df.columns).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT")

		logging.info("success to convert_remesas_to_df")

	def write_files_csv_gcs(self):

		#df_names_files = self.df_anulation.select("NAME_FILE").distinct()
		#df_names_files_pd = df_names_files.toPandas()
		#self.arr_names_files = df_names_files_pd.to_numpy()
		list_files = self.df_remesas.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :
			
			try:
				self.df_remesas.filter(self.df_remesas.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"REMESAS"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy remesas-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy remesas-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_remesas.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):
		self.convert_remesas_to_df()
		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")



