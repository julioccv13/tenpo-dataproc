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


class Portal_paypal:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process
		os.system('mkdir portal_paypal_files')
		get_files = "gsutil -m cp {0} ./portal_paypal_files/".format(source)
		os.system(get_files)
		os.system("hdfs dfs -put portal_paypal_files /")

	def convert_paypal_to_df(self):

		schema_final = StructType([
				StructField("DATE_PROCESS_FILE", StringType(), True),
				StructField("NAME_FILE", StringType(), True),
				StructField("ID_ROW", StringType(), True),
				StructField("FECHA", StringType(), True),
				StructField("HORA", StringType(), True),
				StructField("ZONA_HORARIA", StringType(), True),
				StructField("NOMBRE", StringType(), True),
				StructField("TIPO", StringType(), True),
				StructField("ESTADO", StringType(), True),
				StructField("DIVISA", StringType(), True),
				StructField("BRUTO", StringType(), True),
				StructField("COMISION", StringType(), True),
				StructField("NETO", StringType(), True),
				StructField("CORREO_ELECTRONICO_DEL_REMITENTE", StringType(), True),
				StructField("CORREO_ELECTRONICO_DEL_DESTINATARIO", StringType(), True),
				StructField("ID_DE_TRANSACCION", StringType(), True),
				StructField("ESTADO_DE_LA_CONTRAPARTE", StringType(), True),
				StructField("DIRECCION_DE_ENVIO", StringType(), True),
				StructField("ESTADO_DE_LA_DIRECCION", StringType(), True),
				StructField("NOMBRE_DEL_ARTICULO", StringType(), True),
				StructField("ID_DEL_ARTICULO", StringType(), True),
				StructField("IMPORTE_DE_ENVIO_Y_MANIPULACION", StringType(), True),
				StructField("IMPORTE_DEL_SEGURO", StringType(), True),
				StructField("IMPUESTO_SOBRE_VENTAS", StringType(), True),
				StructField("NOMBRE_DE_LA_OPCION_1", StringType(), True),
				StructField("VALOR_DE_LA_OPCION_1", StringType(), True),
				StructField("NOMBRE_DE_LA_OPCION_2", StringType(), True),
				StructField("VALOR_DE_LA_OPCION_2", StringType(), True),
				StructField("SITIO_DE_SUBASTAS", StringType(), True),
				StructField("ID_DEL_COMPRADOR", StringType(), True),
				StructField("URL_DEL_ARTICULO", StringType(), True),
				StructField("FECHA_DE_CIERRE", StringType(), True),
				StructField("ID_DEPOSITARIO", StringType(), True),
				StructField("ID_DE_REFERENCIA_DE_LA_TRANSACCION", StringType(), True),
				StructField("NUMERO_DE_FORMATO_DE_PAGO31", StringType(), True),
				StructField("NUMERO_PERSONALIZADO", StringType(), True),
				StructField("CANTIDAD", StringType(), True),
				StructField("ID_DEL_FORMATO_DE_PAGO", StringType(), True),
				StructField("SALDO", StringType(), True),
				StructField("DIRECCION", StringType(), True),
				StructField("DIRECCION_CONTINUACION_DISTRITO_BARRIO", StringType(), True),
				StructField("POBLACION_O_CIUDAD", StringType(), True),
				StructField("ESTADO_PROVINCIA_REGION_CONDADO_TERRITORIO_PREFECTURA_REPUBLICA", StringType(), True),
				StructField("CODIGO_POSTAL", StringType(), True),
				StructField("PAIS", StringType(), True),
				StructField("NUMERO_DE_TELEFONO_DE_CONTACTO", StringType(), True),
				StructField("ASUNTO", StringType(), True),
				StructField("NOTA", StringType(), True),
				StructField("FUENTE_DE_PAGO", StringType(), True),
				StructField("TIPO_DE_TARJETA", StringType(), True),
				StructField("CODIGO_DE_EVENTO_DE_LA_TRANSACCION", StringType(), True),
				StructField("ID_DE_SEGUIMIENTO_DEL_PAGO", StringType(), True),
				StructField("ID_DE_REFERENCIA_BANCARIA", StringType(), True),
				StructField("CODIGO_DE_PAIS_DEL_COMPRADOR", StringType(), True),
				StructField("DETALLES_DEL_ARTICULO", StringType(), True),
				StructField("CUPONES", StringType(), True),
				StructField("PROMOCIONES_ESPECIALES", StringType(), True),
				StructField("NUMERO_DE_TARJETA_DE_LEALTAD", StringType(), True),
				StructField("ESTADO_DE_LA_REVISION_DE_LA_AUTORIZACION", StringType(), True),
				StructField("REQUISITOS_PARA_LA_PROTECCION", StringType(), True),
				StructField("CODIGO_DE_PAIS", StringType(), True),
				StructField("REPERCUSIONES_EN_EL_SALDO", StringType(), True),
				StructField("CARTERA", StringType(), True),
				StructField("COMENTARIO_1", StringType(), True),
				StructField("COMENTARIO_2", StringType(), True),
				StructField("NUMERO_DE_FORMATO_DE_PAGO62", StringType(), True),
				StructField("NUMERO_DE_PEDIDO", StringType(), True),
				StructField("NUMERO_DE_REFERENCIA_DEL_CLIENTE", StringType(), True),
				StructField("ID_DE_TRANSACCION_PAYFLOW_PNREF", StringType(), True),
				StructField("PROPINA", StringType(), True),
				StructField("DESCUENTO", StringType(), True),
				StructField("ID_DEL_VENDEDOR", StringType(), True),
				StructField("FILTRO_DE_RIESGO", StringType(), True),
				StructField("COMISION_POR_TRANSACCION_DEL_CREDITO", StringType(), True),
				StructField("COMISION_PROMOCIONAL_DEL_CREDITO", StringType(), True),
				StructField("TERMINO_DEL_CREDITO", StringType(), True),
				StructField("TIPO_DE_OFERTA_DEL_CREDITO", StringType(), True),
				StructField("ID_ORIGINAL_DEL_FORMATO_DE_PAGO", StringType(), True),
				StructField("SUBTIPO_DE_FORMA_DE_PAGO", StringType(), True),
				StructField("COMISION_DE_LA_CAMPANA", StringType(), True),
				StructField("NOMBRE_DE_LA_CAMPANA", StringType(), True),
				StructField("DESCUENTO_DE_LA_CAMPANA", StringType(), True),
				StructField("DIVISA_DEL_DESCUENTO_DE_LA_CAMPANA", StringType(), True),
				StructField("CODIGO_DE_RECHAZO", StringType(), True),
				StructField("TRANSACTION_TYPE", StringType(), True)])
		
		schema = StructType(schema_final.fields[3:-1])
		
		data = []
		self.df_portal_paypal_final = self.spark.createDataFrame(data, schema=schema_final)

		for file in [doc for doc in os.listdir("./portal_paypal_files/")]:
			logging.info("file:"+file)

			file_arr = file.split("_")
			df = self.spark.read.option("sep", ",").option("header", "true").option("schema",schema).csv("hdfs:///portal_paypal_files/"+file)
			df = df.toDF(*schema.fieldNames())
				
			if file_arr[0][:6].lower() == "abonos":
				df_portal_paypal = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("TRANSACTION_TYPE", lit("ABONOS")).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE", "NAME_FILE","ID_ROW", *df.columns, "TRANSACTION_TYPE").drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT")
				

			elif file_arr[0][:7].lower() == "retiros":
				df_portal_paypal = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("TRANSACTION_TYPE", lit("RETIROS")).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).select("DATE_PROCESS_FILE", "NAME_FILE","ID_ROW", *df.columns, "TRANSACTION_TYPE").drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT")

			else :
				logging.info("new name format file")
				sys.exit(1)

			self.df_portal_paypal_final = self.df_portal_paypal_final.union(df_portal_paypal)


		logging.info("success to convert_paypal_to_df")

	def write_files_csv_gcs(self):

		list_files = self.df_portal_paypal_final.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :
			
			try:
				self.df_portal_paypal_final.filter(self.df_portal_paypal_final.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"PAYPAL_PORTAL/"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy paypal-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy paypal-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_portal_paypal_final.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):

		self.convert_paypal_to_df()

		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")



