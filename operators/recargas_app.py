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


class Recargas:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process
		os.system('mkdir recargas_app')
		get_files = "gsutil -m cp {0} ./recargas_app/".format(source)
		os.system(get_files)

	def convert_recargas_to_df(self):

		df1 = pd.read_excel('./recargas_app/reporte_servicio_recargas_-20230816.xlsx')
		df = self.spark.createDataFrame(df1)
		self.df_recargas = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("CODIGO_MC",substring(df._c0,1,10)).withColumn("FECHA_HORA",substring(df._c0,11,19)).withColumn("ID_TERMINAL",substring(df._c0,20,8)).withColumn("OPERADOR",substring(df._c0,21,8)).withColumn("FONO",substring(df._c0,29,9)).withColumn("TIPO_TRX",substring(df._c0,38,11)).withColumn("ESTADO_TRX",substring(df._c0,49,8)).withColumn("COD_RESPUESTA",substring(df._c0,57,2)).withColumn("MSG_RESPUESTA",substring(df._c0,59,20)).withColumn("MONTO",substring(df._c0,79,6)).withColumn("COD_MANDANTE",substring(df._c0,85,9)).withColumn("COD_AUT_MDTE",substring(df._c0,94,12)).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT").drop("_c0")
		
		logging.info("success to convert_recargas_app_to_df")

	def write_files_csv_gcs(self):

		#df_names_files = self.df_anulation.select("NAME_FILE").distinct()
		#df_names_files_pd = df_names_files.toPandas()
		#self.arr_names_files = df_names_files_pd.to_numpy()
		list_files = self.df_recargas.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :
			
			try:
				self.df_recargas.filter(self.df_recargas.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"RECARGAS_APP"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy recargas-app-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy recargas-app-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_recargas.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):

		self.convert_recargas_to_df()

		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")



