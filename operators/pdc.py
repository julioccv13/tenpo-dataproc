from pyspark.sql import SparkSession
import logging
from time import gmtime, strftime
from  pyspark.sql.functions import input_file_name
from google.cloud import bigquery
from google.cloud import storage
import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from uuid import uuid4
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)


class Pdc:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process

	def convert_pdc_to_df(self):

		df = self.spark.read.option("sep", ",").option("header", "true").option("encoding", "ISO-8859-1").csv(self.source)
		#df1= spark.read.option("sep", ",").option("header", "true").option("encoding", "ISO-8859-1").csv("gs://mark-vii-conciliacion/data/PDC/reporte_servicio_PDC-20230730.csv")
		self.df_pdc = df.filter(df.Codigo_MC != "Codigo_MC").withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE","NAME_FILE","ID_ROW","Codigo_MC","Fecha_Hora","Terminal","Rut_Comercio","Operador","Tipo_Tx","Estado_Tx","Monto","Mandante","Codigo_MC_Relacion")
		#df1.withColumn("DATE_PROCESS_FILE",lit(timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE","NAME_FILE_ENTIRE","NAME_FILE","ID_ROW").show(truncate=False)

		logging.info("success to convert_pdc_to_df")

	def write_files_csv_gcs(self):

		list_files = self.df_pdc.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :
			
			try:
				self.df_pdc.filter(self.df_pdc.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"PDC/"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy anulation-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy anulation-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_pdc.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):

		self.convert_pdc_to_df()

		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")

