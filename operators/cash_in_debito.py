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

logging.basicConfig(level=logging.INFO)


class Cash_in_debito:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process

	def convert_cash_in_debito_to_df(self):

		df = self.spark.read.option("header", "false").option("encoding", "ISO-8859-1").csv(self.source)
		self.df_cash_in_debito = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("CCRE",substring(df._c0,1,8)).withColumn("FPRO",substring(df._c0,9,6)).withColumn("FCOM",substring(df._c0,15,6)).withColumn("APPR",substring(df._c0,21,6)).withColumn("LAST_PAN",substring(df._c0,27,19)).withColumn("PAN",when(trim("LAST_PAN") != "",concat(lit("*"*12),substring("LAST_PAN",8,4))).otherwise("LAST_PAN")).withColumn("AMT1",substring(df._c0,46,13)).withColumn("TTRA",substring(df._c0,59,2)).withColumn("CPRI",substring(df._c0,61,8)).withColumn("MARC",substring(df._c0,69,2)).withColumn("FEDI",substring(df._c0,71,8)).withColumn("NRO_UNICO",substring(df._c0,79,26)).withColumn("COM_COMIV",substring(df._c0,105,13)).withColumn("CAD_CADIV",substring(df._c0,118,13)).withColumn("DECOM_IVCOM",substring(df._c0,131,13)).withColumn("DCOAD_IVCOM",substring(df._c0,144,13)).withColumn("PREPAGO",substring(df._c0,157,1)).withColumn("NOMBRE_BANCO",substring(df._c0,158,35)).withColumn("TIPO_CUENTA_BANCO",substring(df._c0,193,2)).withColumn("NUMERO_CUENTA_BANCO",substring(df._c0,195,18)).withColumn("MONEDA_CUENTA_BANCO",substring(df._c0,213,3)).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT").drop("LAST_PAN").drop("_c0")
		
		logging.info("success to convert_cash_in_debito_to_df")

	def write_files_csv_gcs(self):

		#df_names_files = self.df_cash_in_debito.select("NAME_FILE").distinct()
		#df_names_files_pd = df_names_files.toPandas()
		#self.arr_names_files = df_names_files_pd.to_numpy()
		list_files = self.df_cash_in_debito.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :
			
			try:
				self.df_cash_in_debito.filter(self.df_cash_in_debito.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"CASH_IN_DEBITO"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy cash_in_debito-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy cash_in_debito-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_cash_in_debito.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):

		self.convert_cash_in_debito_to_df()

		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")



