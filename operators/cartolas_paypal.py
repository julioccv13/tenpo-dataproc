from pyspark.sql import SparkSession
import logging
from time import gmtime, strftime
<<<<<<< HEAD
from  pyspark.sql.functions import input_file_name
=======
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
from google.cloud import bigquery
from google.cloud import storage
import os
import sys
from pyspark.sql.functions import *
<<<<<<< HEAD
=======
from pyspark.sql.types import *
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
from uuid import uuid4
import numpy as np
import pandas as pd
import openpyxl
import pyxlsb
import glob

logging.basicConfig(level=logging.INFO)


class Cartolas:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process
<<<<<<< HEAD
		os.system('mkdir cartolas; mkdir cartolas_csv')
=======
		os.system('mkdir cartolas; mkdir cartolas_csv; mkdir cartolas_chile; mkdir cartolas_segurity')
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
		get_files = "gsutil -m cp {0} ./cartolas/".format(source)
		os.system(get_files)

	def convert_excel_csv(self):
		schema = [
			"N_OPERACION",
			"FECHA_HORA",
			"CUENTA_DESTINO",
			"ALIAS_DESTINO",
			"RUT_ORIGEN",
			"BANCO_ORIGEN",
			"NOMBRE_ORIGEN",
			"CUENTA_ORIGEN",
			"MONTO"]

		for file in [doc for doc in os.listdir("./cartolas/")]:
			if file.endswith(".xlsx"):
				df= pd.read_excel('./cartolas/'+file, 'Transferencias')
				df.columns = schema
				df.to_csv("./cartolas_csv/"+file,index=False,header=True)
				os.system("hdfs dfs -put ./cartolas_csv/ /")
<<<<<<< HEAD
				logging.info("success to convert_excel_to_csv")
=======
				logging.info("success to convert_excel_to_csv bco_estado")
			
			if file.startswith("consultas-recibidas"):

				os.system(f"cp ./cartolas/{file} ./cartolas_chile/{file}")
				os.system("hdfs dfs -put ./cartolas_chile/ /")

			if file.startswith("RecibidasEmpresas"):
				os.system(f"cp ./cartolas/{file} ./cartolas_segurity/{file}")
				os.system(f"hdfs dfs -put ./cartolas_segurity/ /")


		#os.system("hdfs dfs -ls /cartolas_csv")
		#os.system("hdfs dfs -ls /cartolas_chile")
		#os.system("hdfs dfs -ls /cartolas_segurity")

>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e

	def convert_cartolas_to_df(self):
		
		self.convert_excel_csv()
		df = self.spark.read.options(header='True',delimiter=',',encoding='ISO-8859-1').csv("hdfs:///cartolas_csv/")
<<<<<<< HEAD
		df.show(truncate=False)
		self.df_bco_estado = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE", "NAME_FILE","ID_ROW", *df.columns).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT")

		logging.info("success to convert_cartolas_to_df")


		schema_bco_chile = StructType([
=======
		#df.show(truncate=False)
		self.df_bco_estado = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE", "NAME_FILE","ID_ROW", *df.columns).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT")

		logging.info("success to convert cartolas_bco_estado_to_df")


		schema_bco_chile = StructType([
			StructField("DATE_PROCESS_FILE", StringType(), True),
			StructField("NAME_FILE", StringType(), True),
			StructField("ID_ROW", StringType(), True),
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
			StructField("FECHA", StringType(), True),
			StructField("NOMBRE", StringType(), True),
			StructField("RUT", StringType(), True),
			StructField("BANCO_ORIGEN", StringType(), True),
			StructField("CUENTA_ORIGEN", StringType(), True),
			StructField("TIPO_DE_OPERACION", StringType(), True),
<<<<<<< HEAD
			StructField("CEUNTA_DESTINO", StringType(), True),
=======
			StructField("CUENTA_DESTINO", StringType(), True),
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
			StructField("MONTO", StringType(), True),
			StructField("ID_TRANSACCION", StringType(), True),
			StructField("TIPO_MONEDA", StringType(), True),
			StructField("TIPO_OPERADOR", StringType(), True),
			StructField("COMENTARIO", StringType(), True),])
		
		schema_bco_segurity = StructType([
<<<<<<< HEAD
=======
			StructField("DATE_PROCESS_FILE", StringType(), True),
			StructField("NAME_FILE", StringType(), True),
			StructField("ID_ROW", StringType(), True),
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
			StructField("FECHA", StringType(), True),
			StructField("NOMBRE_EMISOR", StringType(), True),
			StructField("RUT_EMISOR", StringType(), True),
			StructField("CUENTA_ORIGEN", StringType(), True),
			StructField("BANCO_ORIGEN", StringType(), True),
			StructField("MONTO", StringType(), True),
			StructField("ASUNTO", StringType(), True),])

		

<<<<<<< HEAD
		bcochile_files = glob.glob(os.path.join('hdfs:///cartolas/', 'consultas-recibidas*'))
		bcosegurity_files = glob.glob(os.path.join('hdfs:///cartolas/', 'RecibidasEmpresas*'))

		if bcochile_files:
			df = self.spark.read.option("sep", ",").option("header", "true").option("schema",schema_bco_chile).csv("hdfs:///cartolas/consultas-recibidas*")
			self.df_bco_chile = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("TRANSACTION_TYPE", lit("ABONOS")).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE", "NAME_FILE","ID_ROW", *df.columns, "TRANSACTION_TYPE").drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT")
=======
		bco_chile = self.spark.read.option("sep", ",").option("header", "true").option("schema",schema_bco_chile).csv("hdfs:///cartolas_chile/")
		bco_segurity = self.spark.read.option("sep", ",").option("header", "true").option("schema",schema_bco_segurity).csv("hdfs:///cartolas_segurity/")


		if bco_chile.count() > 0:
			self.df_bco_chile = bco_chile.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE", "NAME_FILE","ID_ROW", *bco_chile.columns).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT")
			self.df_bco_chile = self.df_bco_chile.toDF(*schema_bco_chile.fieldNames())
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
		else :
			logging.info("there are no cartolas: bco chile")

		
<<<<<<< HEAD
		if bcosegurity_files:
			df = self.spark.read.option("sep", ",").option("header", "true").option("schema",schema_bco_segurity).csv("hdfs:///cartolas/RecibidasEmpresas*")
			self.df_bco_segurity = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("TRANSACTION_TYPE", lit("ABONOS")).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE", "NAME_FILE","ID_ROW", *df.columns, "TRANSACTION_TYPE").drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT")	
		
=======
		if bco_segurity.count() > 0:
			self.df_bco_segurity = bco_segurity.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE", "NAME_FILE","ID_ROW", *bco_segurity.columns).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT")	
			self.df_bco_segurity = self.df_bco_segurity.toDF(*schema_bco_segurity.fieldNames())
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
		else :
			logging.info("there are no cartolas: bco segurity")		




	def write_files_csv_gcs(self):

<<<<<<< HEAD
		#df_names_files = self.df_anulation.select("NAME_FILE").distinct()
		#df_names_files_pd = df_names_files.toPandas()
		#self.arr_names_files = df_names_files_pd.to_numpy()
=======
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
		list_files_estado = self.df_bco_estado.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()
		list_files_chile = self.df_bco_chile.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()
		list_files_segurity = self.df_bco_segurity.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files_estado:
			try:
				self.df_bco_estado.filter(self.df_bco_estado.NAME_FILE == name_file).write.mode("append").options(header='True', delimiter=',').csv(self.output + "REMESAS" + name_file + "/" + self.timestamp_bucket + "/")
				logging.info(f"success when append bco-estado-csv {name_file} to bucket\n")
			except Exception as e:
				logging.error(f"failed when append bco-estado-csv {name_file} to bucket: {str(e)}\n")

		for name_file in list_files_chile:
			try:
				self.df_bco_chile.filter(self.df_bco_chile.NAME_FILE == name_file).write.mode("append").options(header='True', delimiter=',').csv(self.output + "REMESAS" + name_file + "/" + self.timestamp_bucket + "/")
				logging.info(f"success when append bco-chile-csv {name_file} to bucket\n")
			except Exception as e:
				logging.error(f"failed when append bco-chile-csv {name_file} to bucket: {str(e)}\n")

		for name_file in list_files_segurity:
			try:
				self.df_bco_segurity.filter(self.df_bco_segurity.NAME_FILE == name_file).write.mode("append").options(header='True', delimiter=',').csv(self.output + "REMESAS" + name_file + "/" + self.timestamp_bucket + "/")
				logging.info(f"success when append bco-segurity-csv {name_file} to bucket\n")
			except Exception as e:
				logging.error(f"failed when append bco-segurity-csv {name_file} to bucket: {str(e)}\n")






	def write_df_to_bigquery(self):

<<<<<<< HEAD
		try:
			self.df_bco_estado.write.format("bigquery").option("table", self.output).mode("overwrite").save()
=======

		try:
			self.df_bco_estado.write.format("bigquery").option("table", self.output+'_banco_estado').mode("overwrite").save()
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
			logging.info("success when write to bigquery df_bco_estado \n")

		except Exception as e:
			logging.error("failed when write in bigquery df_bco_estado:"+str(e)+"\n")

		try:
<<<<<<< HEAD
			self.df_bco_chile.write.format("bigquery").option("table", self.output).mode("overwrite").save()
=======
			self.df_bco_chile.write.format("bigquery").option("table", self.output+'_banco_chile').mode("overwrite").save()
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
			logging.info("success when write to bigquery df_bco_chile \n")

		except Exception as e:
			logging.error("failed when write in bigquery df_bco_chile:"+str(e)+"\n")

		try:
<<<<<<< HEAD
			self.df_bco_segurity.write.format("bigquery").option("table", self.output).mode("overwrite").save()
=======
			self.df_bco_segurity.write.format("bigquery").option("table", self.output+'_banco_segurity').mode("overwrite").save()
>>>>>>> 51e3158a3f85eea6cbcb64271dee717181507d7e
			logging.info("success when write to bigquery df_bco_segurity \n")

		except Exception as e:
			logging.error("failed when write in bigquery df_bco_segurity:"+str(e)+"\n")

	def run(self):
		self.convert_cartolas_to_df()
		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")



