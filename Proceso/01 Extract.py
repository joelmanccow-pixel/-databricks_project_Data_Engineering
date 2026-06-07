# ============================================================
# NOTEBOOK: Estraccion raw a bronze
# ============================================================

# ── Parámetros de rutas ──────────────────────────────────────
from pyspark.sql import functions as F
import re

RAW_PATH      = "abfss://raw@adlprojectof.dfs.core.windows.net/"
CATALOG       = "adbprojectfinaljm"
SCHEMA_BRONZE = "bronze"

def clean_column_names(df):
    def clean(name):
        name = name.strip()
        name = re.sub(r'[;{}()\n\t= ]', '_', name)
        name = re.sub(r'_+', '_', name)
        name = name.strip('_')
        return name
    for old in df.columns:
        df = df.withColumnRenamed(old, clean(old))
    return df
print(df_avisos)

# ── 1. Equipos
df_equipos = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ";")
    .csv(f"{RAW_PATH}IH01_Equipos.csv"))
df_equipos = clean_column_names(df_equipos)
df_equipos.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.equipos")
print(f"✅ equipos: {df_equipos.count()} registros")


# ── 2. Avisos
df_avisos = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ";")
    .csv(f"{RAW_PATH}IW29_avisos.csv"))
df_avisos = clean_column_names(df_avisos)
df_avisos.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.avisos")
print(f"✅ avisos: {df_avisos.count()} registros")

# ── 3. Ordenes
df_ordenes = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ";")
    .csv(f"{RAW_PATH}IW39_ordenes.csv"))
df_ordenes = clean_column_names(df_ordenes)
df_ordenes.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.ordenes")
print(f"✅ ordenes: {df_ordenes.count()} registros")

# ── Verificacion
spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.equipos").printSchema()
spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.avisos").printSchema()
spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.ordenes").printSchema()