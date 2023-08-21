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


class Cca:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process

	def convert_cca_to_df(self):

		df = self.spark.read.option("header", "false").option("encoding", "ISO-8859-1").csv(self.source)
		self.df_cca = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("TIPO_REGISTRO",substring(df._c0,1,1)).withColumn("NUMERO_SECUENCIA",substring(df._c0,2,6)).withColumn("TIPO_MENSAJE",substring(df._c0,8,4)).withColumn("CODIGO_TRANSACCION",substring(df._c0,12,6)).withColumn("MONTO_TRANSACCION",substring(df._c0,18,12)).withColumn("TIMESTAMP_CCA",substring(df._c0,30,12)).withColumn("NUMERO_TRACE",substring(df._c0,42,12)).withColumn("FECHA_IFO",substring(df._c0,54,6)).withColumn("HORA_IFO",substring(df._c0,60,6)).withColumn("CODIGO_AUTORIZACION",substring(df._c0,66,6)).withColumn("CODIGO_RESPUESTA",substring(df._c0,72,3)).withColumn("BANCO_ORIGEN",substring(df._c0,75,4)).withColumn("CUENTA_ORIGEN",substring(df._c0,79,20)).withColumn("RUT_GIRADOR",substring(df._c0,99,12)).withColumn("NOMBRE_GIRADOR",substring(df._c0,111,100)).withColumn("BANCO_DESTINO",substring(df._c0,211,4)).withColumn("CUENTA_DESTINO",substring(df._c0,215,20)).withColumn("RUT_DESTINATARIO",substring(df._c0,235,12)).withColumn("NOMBRE_DESTINATARIO",substring(df._c0,247,100)).withColumn("REFERENCIA",substring(df._c0,347,69)).withColumn("INDICADOR_TEF",substring(df._c0,416,1)).withColumn("NUMERO_OPERACION",substring(df._c0,417,30)).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT").drop("_c0")
		
		logging.info("success to convert_anulation_to_df")

	def write_files_csv_gcs(self):

		#df_names_files = self.df_anulation.select("NAME_FILE").distinct()
		#df_names_files_pd = df_names_files.toPandas()
		#self.arr_names_files = df_names_files_pd.to_numpy()
		list_files = self.df_cca.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :
			
			try:
				self.df_cca.filter(self.df_cca.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"CCA"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy cca-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy cca-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_cca.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):

		self.convert_cca_to_df()

		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")



