-- Crear schemas
CREATE SCHEMA IF NOT EXISTS adbprojectfinaljm.bronze;
CREATE SCHEMA IF NOT EXISTS adbprojectfinaljm.silver;
CREATE SCHEMA IF NOT EXISTS adbprojectfinaljm.gold;

-- External Locations
CREATE EXTERNAL LOCATION IF NOT EXISTS `extl-bronze`
  URL 'abfss://bronze@adlprojectof.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `adbprojectfinaljm`);

CREATE EXTERNAL LOCATION IF NOT EXISTS `extl-silver`
  URL 'abfss://silver@adlprojectof.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `adbprojectfinaljm`);

CREATE EXTERNAL LOCATION IF NOT EXISTS `extl-golden`
  URL 'abfss://golden@adlprojectof.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `adbprojectfinaljm`);