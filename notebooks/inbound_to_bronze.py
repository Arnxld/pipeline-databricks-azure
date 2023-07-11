# Databricks notebook source
# MAGIC %md
# MAGIC # Transferindo dados da camada inbound para a camada bronze

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dados/inbound"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo dados da camada inbound

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
# MAGIC val dados = spark.read.json(path)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removendo colunas

# COMMAND ----------

# MAGIC %scala
# MAGIC val dados_anuncio = dados.drop("imagens", "usuario")
# MAGIC display(dados_anuncio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando nova coluna ID
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.col

# COMMAND ----------

# MAGIC %scala
# MAGIC val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando na camada bronze

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
# MAGIC df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

# COMMAND ----------


