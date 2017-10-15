  CREATE EXTERNAL TABLE IF NOT EXISTS weather_raw (
  station         string,
  station_name    string,
  latitude        string,
  longitude       string,
  elevation       string,
  wdate           string,
  awnd            string,
  awnd_attributes string, 
  prcp            decimal(5,1),
  prcp_attributes string, 
  snow            int,
  snow_attributes string, 
  tavg            string,
  tavg_attributes string,
  tmax            string,
  tmax_attributes string,
  tmin            string,
  tmin_attributes string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION 's3://yourS3Bucket/data/' TBLPROPERTIES("skip.header.line.count"="1");



CREATE TABLE weather AS SELECT station, station_name, elevation, latitude, longitude
,cast(wdate AS date) AS dt, prcp, snow, tmax, tmin, awnd FROM weather_raw;

set hive.cli.print.header=true;

select * from weather limit 10;
