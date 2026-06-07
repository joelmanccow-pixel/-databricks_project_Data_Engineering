# ============================================================
# NOTEBOOK: load golden
# ============================================================

from pyspark.sql import functions as F

CATALOG       = "adbprojectfinaljm"
SCHEMA_SILVER = "silver"
SCHEMA_GOLDEN = "gold"

GOLDEN_PATH = "abfss://golden@adlprojectof.dfs.core.windows.net/"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLDEN}")
print(f"✅ Schema {SCHEMA_GOLDEN} listo")

df_equipos = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.equipos")
df_avisos  = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.avisos")
df_ordenes = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.ordenes")

# ════════════════════════════════════════════════════════════
# 1. KPIs por Centro de Coste
# ════════════════════════════════════════════════════════════
df_eq_slim = df_equipos.select(
    F.col("Equipo").alias("eq_Equipo"),
    F.col("Centro_de_coste").alias("eq_Centro_de_coste"),
    F.col("Denominación_de_la_ubicación_técnica").alias("eq_Ubicacion")
)

df_kpi_centro = (df_avisos
    .join(df_ordenes, on="Aviso", how="left")
    .join(df_eq_slim, df_avisos["Equipo"] == F.col("eq_Equipo"), how="left")
    .groupBy("eq_Centro_de_coste", "eq_Ubicacion")
    .agg(
        F.count("Aviso").alias("total_avisos"),
        F.sum(F.when(F.col("aviso_cerrado") == True, 1).otherwise(0)).alias("avisos_cerrados"),
        F.sum(F.when(F.col("aviso_cerrado") == False, 1).otherwise(0)).alias("avisos_pendientes"),
        F.countDistinct(df_ordenes["Orden"]).alias("total_ordenes"),
        F.round(F.sum("Costo_real"), 2).alias("costo_total_mantenimiento"),
        F.round(F.avg("duracion_aviso_horas"), 2).alias("duracion_promedio_aviso_horas"),
        F.round(F.avg("duracion_orden_dias"), 2).alias("duracion_promedio_orden_dias")
    )
    .withColumnRenamed("eq_Centro_de_coste", "Centro_de_coste")
    .withColumnRenamed("eq_Ubicacion", "Ubicacion_tecnica")
    .withColumn("fecha_carga", F.current_timestamp())
)

df_kpi_centro.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", f"{GOLDEN_PATH}kpi_mantenimiento_por_centro") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLDEN}.kpi_mantenimiento_por_centro")
print(f"✅ gold.kpi_mantenimiento_por_centro: {df_kpi_centro.count()} registros")

# ════════════════════════════════════════════════════════════
# 2. Avisos por Tipo y Prioridad
# ════════════════════════════════════════════════════════════
df_tipo_prioridad = (df_avisos
    .groupBy("Clase_de_aviso", "Texto_para_prioridad")
    .agg(
        F.count("Aviso").alias("total_avisos"),
        F.sum(F.when(F.col("aviso_cerrado") == True, 1).otherwise(0)).alias("cerrados"),
        F.sum(F.when(F.col("aviso_cerrado") == False, 1).otherwise(0)).alias("pendientes"),
        F.round(F.avg("duracion_aviso_horas"), 2).alias("duracion_promedio_horas")
    )
    .withColumn("fecha_carga", F.current_timestamp())
)

df_tipo_prioridad.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", f"{GOLDEN_PATH}kpi_avisos_por_tipo_prioridad") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLDEN}.kpi_avisos_por_tipo_prioridad")
print(f"✅ gold.kpi_avisos_por_tipo_prioridad: {df_tipo_prioridad.count()} registros")

# ════════════════════════════════════════════════════════════
# 3. Top 100 Equipos con más Averías
# ════════════════════════════════════════════════════════════
df_eq_info = df_equipos.select(
    F.col("Equipo").alias("eq_Equipo"),
    F.col("Denominación_de_objeto_técnico").alias("Denominacion_equipo"),
    F.col("Denominación_de_la_ubicación_técnica").alias("Ubicacion_tecnica"),
    F.col("Tipo_objeto_tecnico"),
    F.col("Fecha_puesta_servicio")
)

df_top_equipos = (df_avisos
    .join(df_eq_info, df_avisos["Equipo"] == F.col("eq_Equipo"), how="left")
    .groupBy(
        df_avisos["Equipo"],
        "Denominacion_equipo",
        "Ubicacion_tecnica",
        "Tipo_objeto_tecnico"
    )
    .agg(
        F.count("Aviso").alias("total_avisos"),
        F.round(F.avg("duracion_aviso_horas"), 2).alias("duracion_promedio_horas"),
        F.round(F.avg("Prioridad"), 2).alias("prioridad_promedio")
    )
    .orderBy(F.col("total_avisos").desc())
    .limit(100)
    .withColumn("fecha_carga", F.current_timestamp())
)

df_top_equipos.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", f"{GOLDEN_PATH}top_equipos_mayor_mantenimiento") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLDEN}.top_equipos_mayor_mantenimiento")
print(f"✅ gold.top_equipos_mayor_mantenimiento: {df_top_equipos.count()} registros")

# ════════════════════════════════════════════════════════════
# 4. Tendencia Mensual
# ════════════════════════════════════════════════════════════
df_tendencia = (df_ordenes
    .filter(F.col("Fecha_entrada").isNotNull())
    .withColumn("anio", F.year("Fecha_entrada"))
    .withColumn("mes",  F.month("Fecha_entrada"))
    .groupBy("anio", "mes", "Clase_de_orden")
    .agg(
        F.count("Orden").alias("total_ordenes"),
        F.sum(F.when(F.col("orden_completada") == True, 1).otherwise(0)).alias("ordenes_completadas"),
        F.round(F.sum("Costo_real"), 2).alias("costo_total"),
        F.round(F.avg("duracion_orden_dias"), 2).alias("duracion_promedio_dias")
    )
    .orderBy("anio", "mes")
    .withColumn("fecha_carga", F.current_timestamp())
)

df_tendencia.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", f"{GOLDEN_PATH}tendencia_mensual_mantenimiento") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLDEN}.tendencia_mensual_mantenimiento")
print(f"✅ gold.tendencia_mensual_mantenimiento: {df_tendencia.count()} registros")

# ════════════════════════════════════════════════════════════
# Verificación
# ════════════════════════════════════════════════════════════
display(spark.table(f"{CATALOG}.{SCHEMA_GOLDEN}.kpi_mantenimiento_por_centro"))
display(spark.table(f"{CATALOG}.{SCHEMA_GOLDEN}.kpi_avisos_por_tipo_prioridad"))
display(spark.table(f"{CATALOG}.{SCHEMA_GOLDEN}.top_equipos_mayor_mantenimiento"))
display(spark.table(f"{CATALOG}.{SCHEMA_GOLDEN}.tendencia_mensual_mantenimiento"))