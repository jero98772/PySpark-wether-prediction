CREATE EXTERNAL TABLE IF NOT EXISTS weather_data_file (
  time STRING,
  temperature DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
)
LOCATION 's3://ldzfinal/raw/file/'
TBLPROPERTIES ('has_encrypted_data'='false');