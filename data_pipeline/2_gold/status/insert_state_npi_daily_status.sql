INSERT INTO `personal-project-444819.scratch.state_npi_daily_status` (etl_run_date, state_id, state, npi_count, npi_can_service_dementia_patients, npi_remo_engaged)
SELECT 
  CURRENT_DATE() AS etl_run_date,
  state_id, 
  state, 
  CAST(npi_count AS INT64) AS npi_count,
  CAST(npi_can_service_dementia_patients AS INT64) AS npi_can_service_dementia_patients,
  CAST(npi_remo_engaged AS INT64) AS npi_remo_engaged
FROM `personal-project-444819.scratch.state_npi_facts`;
