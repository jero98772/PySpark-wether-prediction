CREATE EXTERNAL TABLE IF NOT EXISTS weather_data_api (
  time STRING,
  temperature_2m DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
)
LOCATION 's3://ldzfinal/raw/api/'
TBLPROPERTIES ('has_encrypted_data'='false');