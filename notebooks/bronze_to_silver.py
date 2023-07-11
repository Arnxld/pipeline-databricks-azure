# Databricks notebook source
# MAGIC %md
# MAGIC # Mover dados da camada bronze para a camada silver
# MAGIC - Converter cada campo do JSON em uma coluna individual
# MAGIC - Remover colunas que contenham informações sobre as caracteristicas dos imóveis, não são necessárias
# MAGIC
# MAGIC - os dados devem ser salvos na camada silver no formato delta

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("/mnt/dados/bronze/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo dados na camada bronze

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
# MAGIC val df = spark.read.format("delta").load(path)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformando campos do json em colunas

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df.select("anuncio.*"))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(
# MAGIC   df.select("anuncio.*", "anuncio.endereco.*")
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
# MAGIC display(df_silver) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando na camada Silver

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
# MAGIC df_silver.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------


