-- GRANTS sobre Catalog
GRANT USE CATALOG ON CATALOG adbprojectfinaljm TO `account users`;

-- GRANTS Bronze
GRANT USE SCHEMA ON SCHEMA adbprojectfinaljm.bronze TO `account users`;
GRANT SELECT ON SCHEMA adbprojectfinaljm.bronze TO `account users`;
GRANT SELECT ON TABLE adbprojectfinaljm.bronze.equipos TO `account users`;
GRANT SELECT ON TABLE adbprojectfinaljm.bronze.avisos TO `account users`;
GRANT SELECT ON TABLE adbprojectfinaljm.bronze.ordenes TO `account users`;

-- GRANTS Silver
GRANT USE SCHEMA ON SCHEMA adbprojectfinaljm.silver TO `account users`;
GRANT SELECT ON SCHEMA adbprojectfinaljm.silver TO `account users`;
GRANT SELECT ON TABLE adbprojectfinaljm.silver.equipos TO `account users`;
GRANT SELECT ON TABLE adbprojectfinaljm.silver.avisos TO `account users`;
GRANT SELECT ON TABLE adbprojectfinaljm.silver.ordenes TO `account users`;

-- GRANTS Golden
GRANT USE SCHEMA ON SCHEMA adbprojectfinaljm.gold TO `account users`;
GRANT SELECT ON SCHEMA adbprojectfinaljm.gold TO `account users`;
GRANT SELECT ON TABLE adbprojectfinaljm.gold.kpi_mantenimiento_por_centro TO `account users`;
GRANT SELECT ON TABLE adbprojectfinaljm.gold.kpi_avisos_por_tipo_prioridad TO `account users`;
GRANT SELECT ON TABLE adbprojectfinaljm.gold.top_equipos_mayor_mantenimiento TO `account users`;
GRANT SELECT ON TABLE adbprojectfinaljm.gold.tendencia_mensual_mantenimiento TO `account users`;