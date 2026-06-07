-- DROP tablas Gold
DROP TABLE IF EXISTS adbprojectfinaljm.gold.kpi_mantenimiento_por_centro;
DROP TABLE IF EXISTS adbprojectfinaljm.gold.kpi_avisos_por_tipo_prioridad;
DROP TABLE IF EXISTS adbprojectfinaljm.gold.top_equipos_mayor_mantenimiento;
DROP TABLE IF EXISTS adbprojectfinaljm.gold.tendencia_mensual_mantenimiento;

-- DROP tablas Silver
DROP TABLE IF EXISTS adbprojectfinaljm.silver.equipos;
DROP TABLE IF EXISTS adbprojectfinaljm.silver.avisos;
DROP TABLE IF EXISTS adbprojectfinaljm.silver.ordenes;

-- DROP tablas Bronze
DROP TABLE IF EXISTS adbprojectfinaljm.bronze.equipos;
DROP TABLE IF EXISTS adbprojectfinaljm.bronze.avisos;
DROP TABLE IF EXISTS adbprojectfinaljm.bronze.ordenes;

-- DROP schemas
DROP SCHEMA IF EXISTS adbprojectfinaljm.gold;
DROP SCHEMA IF EXISTS adbprojectfinaljm.silver;
DROP SCHEMA IF EXISTS adbprojectfinaljm.bronze;