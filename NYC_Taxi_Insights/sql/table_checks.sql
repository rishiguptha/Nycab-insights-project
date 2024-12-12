SELECT
  COUNT(*)
FROM
  `nycab-insights-project.nycab_dwh.vendor_dim`;

SELECT
  COUNT(*)
FROM
  `nycab-insights-project.nycab_dwh.fact_trips`;


SELECT
  COUNT(*)
FROM
  `nycab-insights-project.nycab_dwh.datetime_dim`;


SELECT
  COUNT(*)
FROM
  `nycab-insights-project.nycab_dwh.Ratecode_dim`;

SELECT
  COUNT(*)
FROM
  `nycab-insights-project.nycab_dwh.location_dim`;

SELECT
  COUNT(*)
FROM
  `nycab-insights-project.nycab_dwh.payment_type_dim`;




TRUNCATE TABLE `nycab-insights-project.nycab_dwh.vendor_dim`;
TRUNCATE TABLE `nycab-insights-project.nycab_dwh.datetime_dim`;
TRUNCATE TABLE `nycab-insights-project.nycab_dwh.Ratecode_dim`;
TRUNCATE TABLE `nycab-insights-project.nycab_dwh.location_dim`;
TRUNCATE TABLE `nycab-insights-project.nycab_dwh.payment_type_dim`;
TRUNCATE TABLE `nycab-insights-project.nycab_dwh.fact_trips`;
