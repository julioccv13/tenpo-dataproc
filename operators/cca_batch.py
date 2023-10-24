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
import xlrd

logging.basicConfig(level=logging.INFO)


class Cca_batch:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process
		os.system('mkdir cca_batch; mkdir cca_batch_csv')
		get_files = "gsutil -m cp {0} ./cca_batch/".format(source)
		os.system(get_files)

	def convert_excel_csv(self):

		for file in [doc for doc in os.listdir("./cca_batch/")]:
			df= pd.read_excel('./cca_batch/'+file,header=None)
			skip_rows = df[df[0].str.strip() == 'Fecha'].index
			cols = ['Fecha', 'Detalle_Movimiento', 'Nro_documento', 'Cheques_o_cargos','Depositos_o_abonos','Saldo']
			df1 = pd.read_excel('./cca_batch/'+file,header=None,skiprows=skip_rows[0]+1,names=cols)
			df1['Nro_cuenta']='9104924'
			df1['Moneda']='Pesos'
			df1.to_csv("./cca_batch_csv/"+file,index=False,header=True)
			os.system("hdfs dfs -put ./cca_batch_csv/ /")
			logging.info("success to convert_excel_to_csv")

	def convert_cca_batch_to_df(self):

		self.convert_excel_csv()
		df = self.spark.read.options(header='True',delimiter=',',encoding='ISO-8859-1').csv("hdfs:///cca_batch_csv/")
		df.show(truncate=False)
		self.df_cca_batch = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("NAME_FILE",regexp_replace("NAME_FILE","%20"," ")).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE", "NAME_FILE","ID_ROW", *df.columns).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT")

		logging.info("success to convert_cca_batch_to_df")

	def write_files_csv_gcs(self):

		list_files = self.df_cca_batch.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :
			
			try:
				self.df_cca_batch.filter(self.df_cca_batch.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"RECARGAS_APP"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy cca-batch-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy cca-batch-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_cca_batch.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):
		self.convert_cca_batch_to_df()
		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")

