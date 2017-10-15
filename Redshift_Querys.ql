create schema staging;

drop table if exists staging.ord_flights ;
CREATE TABLE staging.ord_flights 
(
  ID                    BIGINT identity(0,1),
  YEAR                  SMALLINT,
  QUARTER               SMALLINT,
  MONTH                 SMALLINT,
  DAY_OF_MONTH          SMALLINT,
  DAY_OF_WEEK           SMALLINT,
  FL_DATE               DATE,
  UNIQUE_CARRIER        VARCHAR(10),
  AIRLINE_ID            INT,
  CARRIER               VARCHAR(4),
  TAIL_NUM              VARCHAR(8),
  FL_NUM                VARCHAR(4),
  ORIGIN_AIRPORT_ID     SMALLINT,
  ORIGIN                VARCHAR(5),
  ORIGIN_CITY_NAME      VARCHAR(35),
  ORIGIN_STATE_FIPS     VARCHAR(10),
  ORIGIN_WAC            VARCHAR(2),
  DEST_AIRPORT_ID       SMALLINT,
  DEST                  VARCHAR(5),
  DEST_CITY_NAME        VARCHAR(35),
  "DEST_STATE_ABR"      VARCHAR(2),
  "DEST_STATE_FIPS"     VARCHAR(10),
  "DEST_WAC"            VARCHAR(2),
  CRS_DEP_TIME          SMALLINT,
  DEP_TIME              VARCHAR(6),
  DEP_DELAY             NUMERIC(22,6),
  DEP_DELAY_NEW         NUMERIC(22,6),
  DEP_DEL15             NUMERIC(22,6),
  DEP_DELAY_GROUP       SMALLINT,
  DEP_TIME_BLK          VARCHAR(15),
  TAXI_OUT              NUMERIC(22,6),
  TAXI_IN               NUMERIC(22,6),
  CRS_ARR_TIME          NUMERIC(22,6),
  ARR_TIME              VARCHAR(6),
  ARR_DELAY             NUMERIC(22,6),
  ARR_DELAY_NEW         NUMERIC(22,6),
  ARR_DEL15             NUMERIC(22,6),
  ARR_DELAY_GROUP       SMALLINT,
  ARR_TIME_BLK          VARCHAR(15),
  CANCELLED             NUMERIC(22,6),
  DIVERTED              NUMERIC(22,6),
  CRS_ELAPSED_TIME      NUMERIC(22,6),
  ACTUAL_ELAPSED_TIME   NUMERIC(22,6),
  AIR_TIME              NUMERIC(22,6),
  FLIGHTS               NUMERIC(22,6),
  DISTANCE              NUMERIC(22,6),
  DISTANCE_GROUP        NUMERIC(22,6),
  CARRIER_DELAY         NUMERIC(22,6),
  WEATHER_DELAY         NUMERIC(22,6),
  NAS_DELAY             NUMERIC(22,6),
  SECURITY_DELAY        NUMERIC(22,6),
  LATE_AIRCRAFT_DELAY   NUMERIC(22,6),
  PRIMARY KEY (id)
)
;


copy staging.ord_flights  
from 's3://yourS3Bucket/data/flightdata.csv'  
CREDENTIALS 'aws_access_key_id=your_access_key;aws_secret_access_key=your_secret_Access_key' 
csv IGNOREHEADER 1;



create or replace function f_days_from_holiday (year int, month int, day int)
returns int
stable
as $$
  import datetime
  from datetime import date
  import dateutil
  from dateutil.relativedelta import relativedelta

  fdate = date(year, month, day)

  fmt = '%Y-%m-%d'
  s_date = fdate - dateutil.relativedelta.relativedelta(days=7)
  e_date = fdate + relativedelta(months=1)
  start_date = s_date.strftime(fmt)
  end_date = e_date.strftime(fmt)

  """
  Compute a list of holidays over a period (7 days before, 1 month after) for the flight date
  """
  from pandas.tseries.holiday import USFederalHolidayCalendar
  calendar = USFederalHolidayCalendar()
  holidays = calendar.holidays(start_date, end_date)
  days_from_closest_holiday = [int((abs(fdate - hdate)).days) for hdate in holidays.date.tolist()]
  return 0 if len(days_from_closest_holiday) == 0 else min(days_from_closest_holiday)
$$ language plpythonu;



SELECT dep_delay_group, DAY_OF_MONTH, DAY_OF_WEEK, FL_DATE, f_days_from_holiday(year,month,day_of_month) AS DAYS_TO_HOLIDAY,
       UNIQUE_CARRIER, FL_NUM, SUBSTRING(DEP_TIME,1,2) AS DEP_HOUR, CAST(DEP_DEL15 AS SMALLINT), CAST(AIR_TIME AS INTEGER),
       CAST(FLIGHTS AS SMALLINT), CAST(DISTANCE AS SMALLINT) 
FROM staging.ord_flights
WHERE origin = 'ORD' AND   cancelled = 0 limit 10;


