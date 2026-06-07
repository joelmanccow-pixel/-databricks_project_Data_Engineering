# ============================================================
# REVERSION: Drop de tablas y schemas
# ============================================================

CATALOG = "adbprojectfinaljm"

tablas_gold = [
    "kpi_mantenimiento_por_centro",
    "kpi_avisos_por_tipo_prioridad",
    "top_equipos_mayor_mantenimiento",
    "tendencia_mensual_mantenimiento"
]

tablas_silver_bronze = ["equipos", "avisos", "ordenes"]

for tabla in tablas_gold:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.gold.{tabla}")
    print(f"✅ DROP gold.{tabla}")

for tabla in tablas_silver_bronze:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.silver.{tabla}")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.bronze.{tabla}")
    print(f"✅ DROP silver/bronze.{tabla}")

spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.gold")
spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.silver")
spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.bronze")
print("✅ Schemas eliminados")