from pyspark.sql import SparkSession
import logging
from time import gmtime, strftime
from  pyspark.sql.functions import input_file_name
from google.cloud import bigquery
from google.cloud import storage
import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from uuid import uuid4
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)


class Cash_in_credito:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process

	def convert_cash_in_credito_to_df(self):

		w = Window().orderBy(lit('A'))
		df = self.spark.read.option("header", "false").option("encoding", "ISO-8859-1").csv(self.source)
		self.df_cash_in_credito = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("NUMC",substring(df._c0,1,8)).withColumn("FPROC",substring(df._c0,9,8)).withColumn("FCOM",substring(df._c0,17,8)).withColumn("MICR",substring(df._c0,25,8)).withColumn("LAST_PAN",substring(df._c0,33,19)).withColumn("NUMTA",when(length(trim("LAST_PAN")) < 6, "LAST_PAN").when(trim("LAST_PAN") == "", "LAST_PAN").otherwise(concat(lit("*"*12),substring("LAST_PAN",8,4)))).withColumn("MARCA",substring(df._c0,52,2)).withColumn("MONTO",substring(df._c0,54,11)).withColumn("MONEDA",substring(df._c0,65,1)).withColumn("TXS",substring(df._c0,66,2)).withColumn("RETE",substring(df._c0,68,4)).withColumn("CPRI",substring(df._c0,72,8)).withColumn("FPAGO",substring(df._c0,80,8)).withColumn("ORPEDI",substring(df._c0,88,26)).withColumn("CODAUT",substring(df._c0,114,6)).withColumn("CUOTAS",substring(df._c0,120,2)).withColumn("VCI",substring(df._c0,122,4)).withColumn("CEIC",substring(df._c0,126,11)).withColumn("CAEICA",substring(df._c0,137,11)).withColumn("DCEIC",substring(df._c0,148,11)).withColumn("DCAEICA",substring(df._c0,159,11)).withColumn("NTC",substring(df._c0,170,2)).withColumn("NOMBRE_BANCO",substring(df._c0,172,35)).withColumn("TIPO_CUENTA_BANCO",substring(df._c0,207,2)).withColumn("NUMERO_CUENTA_BANCO",substring(df._c0,209,18)).withColumn("MONEDA_CUENTA_BANCO",substring(df._c0,227,3)).filter('CAEICA == "00000000000"').drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT").drop("LAST_PAN").drop("_c0")

		logging.info("success to convert_cash_in_credito_to_df")

	def write_files_csv_gcs(self):

		#df_names_files = self.df_cash_in_credito.select("NAME_FILE").distinct()
		#df_names_files_pd = df_names_files.toPandas()
		#self.arr_names_files = df_names_files_pd.to_numpy()
		list_files = self.df_cash_in_credito.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :
			
			try:
				self.df_cash_in_credito.filter(self.df_cash_in_credito.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"CASH_IN_CREDITO"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy cash_in_credito-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy cash_in_credito-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_cash_in_credito.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):

		self.convert_cash_in_credito_to_df()

		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")



