# ETL Mantenimiento de Equipos - Databricks

## Descripción
ETL desarrollado en Azure Databricks utilizando arquitectura Medallion (Bronze → Silver → Gold)
para el análisis de gestión de mantenimiento de equipos industriales (bombas, tanques, refrigeración)
a través de avisos y órdenes de mantenimiento preventivo y correctivo.

## Datasets
- **IH01_Equipos.csv** - Maestro de equipos por planta y centro de coste
- **IW29_avisos.csv** - Avisos de mantenimiento preventivo y correctivo
- **IW39_ordenes.csv** - Órdenes de mantenimiento con costos y tiempos

## Arquitectura
- **Raw** → Azure Data Lake (External Location con Managed Identity)
- **Bronze** → Ingesta directa desde Raw (Delta Tables)
- **Silver** → Limpieza, tipado y estandarización
- **Gold** → KPIs y tablas analíticas para dashboard

## Estructura del repositorio
- `datasets/` - Datasets fuente del ETL
- `proceso/` - Notebooks del ETL (.py)
- `PrepAmb/` - Scripts de preparación de ambiente
- `seguridad/` - Scripts de GRANTS
- `reversion/` - Scripts de DROP tablas
- `.github/workflows/` - CI/CD pipeline
- `dashboard/` - Evidencia del dashboard
- `evidencias/` - Capturas de ejecuciones
- `certificaciones/` - Certificaciones del proyecto

## Servicios Azure utilizados
- Azure Data Lake Storage Gen2
- Azure Databricks (Unity Catalog)
- Azure Key Vault
- Azure Data Factory
- Power BI
