### load libs
library(data.table)

###list files
setwd("./kinematics/")
fileNames <- list.files(paste0(getwd()))
shipID <- fread("ship_id.csv")
shipID_passenger <- shipID[type %like% "Passenger" | type %like% "passenger"]


###get data
# dataList <- lapply(fileNames, fread)

###unify list
# longKinetics <- rbindlist(dataList)

###save to binary
# saveRDS(longKinetics,"longKinetics.rds")

###Import RDS
system.time(longKinetics <- readRDS("longKinetics.rds"))

##all ais type are position reports 
# table(longKinetics$type)
# 1        2        3       18       19 
# 60328736   486998  8337485   645770    10318 
longKinetics[,type:=NULL]

##keep only boats with status=0 "under way using engine"
longKinetics <- longKinetics[status==0]


##set primary key for shipID
setkeyv(shipID_passenger,"mmsi")

##set primary key for kinetics
setkeyv(longKinetics,"mmsi")

##merge  shipid_passenger with kinetics 
fullKinetics <- merge(longKinetics,shipID_passenger)


##turn UNIX timestamps into datetime R types 
fullKinetics[,timestamp:=as.POSIXct(as.numeric(as.character(timestamp)),
                                               origin="1970-01-01",
                                               tz="Europe/Athens")]


##delete initial kinetics table
rm(longKinetics)

