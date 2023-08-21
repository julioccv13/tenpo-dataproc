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


class Anulation:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process

	def convert_anulation_to_df(self):

		df = self.spark.read.option("header", "false").option("encoding", "ISO-8859-1").csv(self.source)
		self.df_anulation = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("ENTIDAD",substring(df._c0,1,4)).withColumn("TIPO_FRANQUICIA",substring(df._c0,5,4)).withColumn("TIPO_MENSAJE",substring(df._c0,9,4)).withColumn("CODIGO_PROCESO",substring(df._c0,13,6)).withColumn("SECUENCIA",substring(df._c0,19,12)).withColumn("SECUENCIA_INC",substring(df._c0,31,12)).withColumn("SECUENCIA_CINTA",substring(df._c0,43,12)).withColumn("LAST_PAN",substring(df._c0,55,19)).withColumn("PAN",when(trim("LAST_PAN") != "",concat(substring("LAST_PAN",0,6),lit("*"*6),substring("LAST_PAN",13,4))).otherwise("LAST_PAN")).withColumn("TIPO_FACTURA",substring(df._c0,74,4)).withColumn("SIGNO_TIPO_FAC",substring(df._c0,78,1)).withColumn("TIPO_INC",substring(df._c0,79,3)).withColumn("NUMERO_REF",substring(df._c0,82,23)).withColumn("FECHA_OPERACION",substring(df._c0,105,10)).withColumn("IMPORTE_OPE",substring(df._c0,115,17)).withColumn("MONEDA_OPE",substring(df._c0,132,3)).withColumn("IMPORTE_COMPENS",substring(df._c0,135,17)).withColumn("MONEDA_COMPENS",substring(df._c0,152,3)).withColumn("CMB_APLI",substring(df._c0,155,9)).withColumn("IMP_EMI",substring(df._c0,164,17)).withColumn("MONEDA_EMI",substring(df._c0,181,3)).withColumn("NUMERO_AUT",substring(df._c0,184,6)).withColumn("COMERCIO",substring(df._c0,190,15)).withColumn("NOMBRE_COMERCIO",substring(df._c0,205,27)).withColumn("POB_COMERCIO",substring(df._c0,232,25)).withColumn("PAIS",substring(df._c0,257,3)).withColumn("COD_ACT",substring(df._c0,260,4)).withColumn("TASA_NAC/INTER",substring(df._c0,264,17)).withColumn("SIGNO_NAC/INTER",substring(df._c0,281,1)).withColumn("TIPO_TRANSACCION",substring(df._c0,282,1)).withColumn("COD_RAZON",substring(df._c0,283,4)).withColumn("COD_RAZON_CHA/ALF",substring(df._c0,287,4)).withColumn("COD_FUNCION_FRAN",substring(df._c0,291,3)).withColumn("NUMERO_ORDEN",substring(df._c0,294,2)).withColumn("IND_CHA_TOT_PAR",substring(df._c0,296,1)).withColumn("IND_ANUL",substring(df._c0,297,1)).withColumn("COD_RESP_PETICION",substring(df._c0,298,1)).withColumn("TIPO_DOCUMENTACION",substring(df._c0,299,1)).withColumn("IND_DOCUMENTACION",substring(df._c0,300,1)).withColumn("IND_ORI_OPE",substring(df._c0,301,1)).withColumn("IND_OPE_ANU",substring(df._c0,302,1)).withColumn("GAS_OPER",substring(df._c0,303,17)).withColumn("SIGNO_GAS",substring(df._c0,320,1)).withColumn("COD_ACT_ESP",substring(df._c0,321,4)).withColumn("TEXTO",substring(df._c0,325,50)).withColumn("SESION_RED",substring(df._c0,375,12)).withColumn("FECHA_LIQ",substring(df._c0,387,10)).withColumn("IND_CUOTAS",substring(df._c0,397,1)).withColumn("NUM_CUOTAS",substring(df._c0,398,2)).withColumn("ICA_DESTINO",substring(df._c0,400,4)).withColumn("BIN_DESTINO",substring(df._c0,404,6)).withColumn("FECHA_CINTA",substring(df._c0,410,10)).withColumn("COD_MAR",substring(df._c0,420,2)).withColumn("IND_TIPT",substring(df._c0,422,2)).withColumn("IND_ERROR",substring(df._c0,424,1)).withColumn("CLAVE_RED",substring(df._c0,425,40)).withColumn("MESES_CARENCIA",substring(df._c0,465,2)).withColumn("TIPO_CUOTA",substring(df._c0,467,4)).withColumn("ID_TERMINAL",substring(df._c0,471,16)).withColumn("FEC_CONTA",substring(df._c0,487,10)).withColumn("SIAIDCD",substring(df._c0,497,19)).withColumn("TIPO_CONTRATO",substring(df._c0,516,2)).withColumn("LINREF",substring(df._c0,518,8)).withColumn("FORPAGO",substring(df._c0,526,2)).withColumn("HOR_OPER",substring(df._c0,528,8)).withColumn("IND_APLI",substring(df._c0,536,1)).withColumn("PORT_INT",substring(df._c0,537,7)).withColumn("IMP_CUOTA",substring(df._c0,544,17)).withColumn("IMP_TOTAL_CUOTA",substring(df._c0,561,17)).withColumn("PROPINA",substring(df._c0,578,17)).withColumn("IMPUESTO",substring(df._c0,595,17)).withColumn("TIP_FRAN_RES",substring(df._c0,612,4)).withColumn("BASE_CALC_IMPUESTO",substring(df._c0,616,17)).withColumn("IMPUESTO_EXT",substring(df._c0,633,17)).withColumn("IND_TIP_AUT",substring(df._c0,650,1)).withColumn("CICLO_VIDA",substring(df._c0,651,15)).withColumn("IND_TASA_CERO",substring(df._c0,666,1)).withColumn("NUMERO_CUOTA",substring(df._c0,667,3)).withColumn("TIPO_VENTA",substring(df._c0,670,1)).withColumn("TIPO_OPERACION",substring(df._c0,671,1)).withColumn("FILLER",substring(df._c0,672,29)).withColumn("FILLER1",substring(df._c0,701,1)).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT").drop("LAST_PAN").drop("_c0")
		
		logging.info("success to convert_anulation_to_df")

	def write_files_csv_gcs(self):

		#df_names_files = self.df_anulation.select("NAME_FILE").distinct()
		#df_names_files_pd = df_names_files.toPandas()
		#self.arr_names_files = df_names_files_pd.to_numpy()
		list_files = self.df_anulation.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :
			
			try:
				self.df_anulation.filter(self.df_anulation.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"OPD/ANULATION/"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy anulation-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy anulation-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_anulation.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):

		self.convert_anulation_to_df()

		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")



