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


class Pos:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process
		os.system('mkdir pos_files')
		get_files = "gsutil -m cp {0} ./pos_files/".format(source)
		os.system(get_files)
		os.system("hdfs dfs -put pos_files /")

	def convert_pos_to_df(self):

		schema = StructType([
			     StructField("DATE_PROCESS_FILE", StringType(), True),
			     StructField("NAME_FILE", StringType(), True),
			     StructField("ID_ROW", StringType(), True),
			     StructField("CODIGO_MC", StringType(), True),
			     StructField("FECHA_TRX", StringType(), True),
			     StructField("ID_CLIENTE", StringType(), True),
			     StructField("MONTO", StringType(), True),
			     StructField("ID_PROCESS", StringType(), True),
			     StructField("TYPE_PROCESS", StringType(), True),
			     StructField("STATUS", StringType(), True)
			     ])
		data = []

		self.df_pos_final = self.spark.createDataFrame(data, schema=schema)

		for file in [doc for doc in os.listdir("./pos_files/")]:
			logging.info("file:"+file)

			file_arr = file.split("_")
			df = self.spark.read.option("sep", ";").option("header", "true").option("encoding", "ISO-8859-1").csv("hdfs:///pos_files/"+file)

			if  file_arr[1] == "cargas" and file_arr[2] != "reversadas" and file_arr[2] != "rechazadas":
				df_pos = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("ID_PROCESS",col("CARGA_ID")).withColumn("MONTO",col("MONTO_CARGA")).withColumn("TYPE_PROCESS",lit("CARGAS")).withColumn("STATUS",lit("OK")).select("DATE_PROCESS_FILE","NAME_FILE","ID_ROW","CODIGO_MC","FECHA_TRX","ID_CLIENTE","MONTO","ID_PROCESS","TYPE_PROCESS","STATUS")
				
			elif file_arr[1] == "cargas" and file_arr[2] =="reversadas" :
				df_pos = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("ID_PROCESS",lit(None)).withColumn("MONTO",col("MONTO_REVERSA_CARGA")).withColumn("TYPE_PROCESS",lit("CARGAS")).withColumn("STATUS",lit("REVERSADAS")).select("DATE_PROCESS_FILE","NAME_FILE","ID_ROW","CODIGO_MC","FECHA_TRX","ID_CLIENTE","MONTO","ID_PROCESS","TYPE_PROCESS","STATUS")
				
			elif file_arr[1] == "cargas" and file_arr[2] == "rechazadas" :
				df_pos = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("ID_PROCESS",col("CARGA_ID")).withColumn("MONTO",col("MONTO_CARGA")).withColumn("TYPE_PROCESS",lit("CARGAS")).withColumn("STATUS",lit("RECHAZADAS")).select("DATE_PROCESS_FILE","NAME_FILE","ID_ROW","CODIGO_MC","FECHA_TRX","ID_CLIENTE","MONTO","ID_PROCESS","TYPE_PROCESS","STATUS")
				
			elif file_arr[1] == "retiros" and file_arr[2] != "reversados" and file_arr[2] != "rechazados" :
				df_pos = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("ID_PROCESS",col("RETIRO_ID")).withColumn("MONTO",col("MONTO_RETIRO")).withColumn("TYPE_PROCESS",lit("RETIROS")).withColumn("STATUS",lit("OK")).select("DATE_PROCESS_FILE","NAME_FILE","ID_ROW","CODIGO_MC","FECHA_TRX","ID_CLIENTE","MONTO","ID_PROCESS","TYPE_PROCESS","STATUS")

			elif file_arr[1] == "retiros" and file_arr[2] == "reversados" :
				df_pos = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("ID_PROCESS",lit(None)).withColumn("MONTO",col("MONTO_REVERSA_RETIRO")).withColumn("TYPE_PROCESS",lit("RETIROS")).withColumn("STATUS",lit("REVERSADAS")).select("DATE_PROCESS_FILE","NAME_FILE","ID_ROW","CODIGO_MC","FECHA_TRX","ID_CLIENTE","MONTO","ID_PROCESS","TYPE_PROCESS","STATUS")

			elif file_arr[1] == "retiros" and file_arr[2] == "rechazados" :
				df_pos = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("ID_PROCESS",col("RETIRO_ID")).withColumn("MONTO",col("MONTO_RETIRO")).withColumn("TYPE_PROCESS",lit("RETIROS")).withColumn("STATUS",lit("RECHAZADAS")).select("DATE_PROCESS_FILE","NAME_FILE","ID_ROW","CODIGO_MC","FECHA_TRX","ID_CLIENTE","MONTO","ID_PROCESS","TYPE_PROCESS","STATUS")

			else :
				logging.info("new name format file")
				sys.exit(1)

			self.df_pos_final = self.df_pos_final.union(df_pos)


		#df = self.spark.read.option("sep", ",").option("header", "true").option("encoding", "ISO-8859-1").csv(self.source)
		#df_pos = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("ID_PROCESS",col("CARGA_ID")).withColumn("MONTO",col("MONTO_CARGA")).select("DATE_PROCESS_FILE","NAME_FILE","ID_ROW","CODIGO_MC","FECHA_TRX","ID_CLIENTE","MONTO","ID_PROCESS")
		#self.df_pos_final = df_pos.withColumn("TYPE_PROCESS", when(col("NAME_FILE").contains("cargas"), "CARGA").when(col("NAME_FILE").contains("retiros"), "RETIRO").otherwise("") ).withColumn("STATUS", when(col("NAME_FILE").contains("rechazadas"), "RECHAZADO").when(col("NAME_FILE").contains("reversadas"), "REVERSADO").when((~col("NAME_FILE").contains("rechazadas")) | (~col("NAME_FILE").contains("reversados")),"OK").otherwise("") ).select("DATE_PROCESS_FILE","NAME_FILE","ID_ROW","CODIGO_MC","FECHA_TRX","ID_CLIENTE","MONTO","ID_PROCESS","TYPE_PROCESS","STATUS")

		logging.info("success to convert_pos_to_df")

	def write_files_csv_gcs(self):

		list_files = self.df_pos_final.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :
			
			try:
				self.df_pos_final.filter(self.df_pos_final.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"POS/"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy anulation-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy anulation-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_pos_final.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):

		self.convert_pos_to_df()

		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")



