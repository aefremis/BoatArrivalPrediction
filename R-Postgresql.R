library(RODBC)
library(data.table)

dsn <- "yourDSN"
userId <- "dbuser"
pass <- "password"


dbhandle <- odbcConnect(dsn = dsn, uid = userId, pwd = pass)

data <- as.data.table(
  sqlQuery(
    dbhandle,'CREATE TABLE passengers_4326 (points_id bigserial Primary key, 
mmsi text references ship_static,
 timestamp timestamp,
 geom geometry(point,4326),
 distance_naut_miles double precision, 
 time_difference double precision, 
 speed real);
INSERT INTO passengers_4326 (timestamp, mmsi, geom)
SELECT to_timestamp(ship_kinematics.timestamp),
ship_static.mmsi, 
ST_SETsRID(ST_MAKEPOINT(lon,lat),4326)
FROM ship_kinematics,ship_static
WHERE ship_static.type like '%assenger%' and ship_static.mmsi=ship_kinematics.mmsi;'))

odbcClose(dbhandle)
