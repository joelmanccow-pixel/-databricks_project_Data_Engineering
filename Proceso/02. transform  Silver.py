# ============================================================
# NOTEBOOK: Limpieza y transformacion - Silver
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

CATALOG       = "adbprojectfinaljm"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"

SILVER_PATH = "abfss://silver@adlprojectof.dfs.core.windows.net/"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")

# Drop para evitar conflicto Managed vs External
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_SILVER}.equipos")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_SILVER}.avisos")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_SILVER}.ordenes")
print("✅ Tablas previas eliminadas")

# ════════════════════════════════════════════════════════════
# 1. SILVER - EQUIPOS
# ════════════════════════════════════════════════════════════
df_equipos = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.equipos")

df_silver_equipos = (df_equipos
    .withColumnRenamed("Tamaño/Dimensión", "Tamanio_Dimension")
    .withColumnRenamed("Tp.objeto_técnico", "Tipo_objeto_tecnico")
    .withColumnRenamed("Fe.puesta_servicio", "Fecha_puesta_servicio")
    .withColumn("Peso_bruto",
        F.regexp_replace(F.col("Peso_bruto"), ",", ".").cast(DoubleType()))
    .withColumn("Valor_de_adquisicion",
        F.regexp_replace(
            F.regexp_replace(F.col("Valor_de_adquisición"), "\\.", ""),
            ",", "."
        ).cast(DoubleType()))
    .drop("Valor_de_adquisición")
    .filter(F.col("Equipo").isNotNull())
    .withColumn("Status_del_sistema", F.upper(F.trim(F.col("Status_del_sistema"))))
    .withColumn("fecha_carga", F.current_timestamp())
)

df_silver_equipos.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", f"{SILVER_PATH}equipos") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.equipos")
print(f"✅ silver.equipos: {df_silver_equipos.count()} registros")

# ════════════════════════════════════════════════════════════
# 2. SILVER - AVISOS
# ════════════════════════════════════════════════════════════
df_avisos = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.avisos")

df_silver_avisos = (df_avisos
    .filter(F.col("Aviso").isNotNull())
    .withColumn("duracion_aviso_horas",
        F.round(
            (F.unix_timestamp("Hora_de_cierre") - F.unix_timestamp("Hora_de_inicio_de_avería")) / 3600,
        2))
    .withColumn("aviso_cerrado",
        F.when(F.col("Cierre_por_fecha").isNotNull(), F.lit(True))
         .otherwise(F.lit(False)))
    .withColumn("Clase_de_aviso", F.upper(F.trim(F.col("Clase_de_aviso"))))
    .withColumn("Texto_para_prioridad", F.upper(F.trim(F.col("Texto_para_prioridad"))))
    .withColumn("fecha_carga", F.current_timestamp())
)

df_silver_avisos.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", f"{SILVER_PATH}avisos") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.avisos")
print(f"✅ silver.avisos: {df_silver_avisos.count()} registros")

# ════════════════════════════════════════════════════════════
# 3. SILVER - ORDENES
# ════════════════════════════════════════════════════════════
df_ordenes = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.ordenes")

df_silver_ordenes = (df_ordenes
    .filter(F.col("Orden").isNotNull())
    .withColumn("Costo_real",
        F.regexp_replace(
            F.regexp_replace(F.col("Total_general_real"), "\\.", ""),
            ",", "."
        ).cast(DoubleType()))
    .drop("Total_general_real")
    .withColumn("duracion_orden_dias",
        F.datediff(F.col("Fecha_real_de_fin_de_la_orden"), F.col("Fecha_inicio_real")))
    .withColumn("orden_completada",
        F.when(F.col("Fecha_real_de_fin_de_la_orden").isNotNull(), F.lit(True))
         .otherwise(F.lit(False)))
    .withColumn("Clase_de_orden", F.upper(F.trim(F.col("Clase_de_orden"))))
    .withColumn("fecha_carga", F.current_timestamp())
)

df_silver_ordenes.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", f"{SILVER_PATH}ordenes") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.ordenes")
print(f"✅ silver.ordenes: {df_silver_ordenes.count()} registros")

# ════════════════════════════════════════════════════════════
# Verificación
# ════════════════════════════════════════════════════════════
display(spark.table(f"{CATALOG}.{SCHEMA_SILVER}.equipos"))
display(spark.table(f"{CATALOG}.{SCHEMA_SILVER}.avisos"))
display(spark.table(f"{CATALOG}.{SCHEMA_SILVER}.ordenes"))