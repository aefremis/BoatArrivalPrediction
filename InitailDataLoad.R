### load libs
library(data.table);library(doParallel);library(foreach)
options(scipen=999)
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
# system.time(longKinetics <- readRDS("longKinetics.rds"))

##all ais type are position reports 
# table(longKinetics$type)
# 1        2        3       18       19 
# 60328736   486998  8337485   645770    10318 
# longKinetics[,type:=NULL]

##keep only boats with status=0 "under way using engine"
# longKinetics <- longKinetics[status==0]

##set primary key for shipID
# setkeyv(shipID_passenger,"mmsi")

##set primary key for kinetics
# setkeyv(longKinetics,"mmsi")

##merge  shipid_passenger with kinetics 
# fullKinetics <- merge(longKinetics,shipID_passenger)


##turn UNIX timestamps into datetime R types 
# fullKinetics[,timestamp:=as.POSIXct(as.numeric(as.character(timestamp)),
                                               # origin="1970-01-01",
                                               # tz="Europe/Athens")]


##delete initial kinetics table
# rm(longKinetics)

###rel frequency
# table(fullKinetics$type)/sum(table(fullKinetics$type))

##variables to keep
# varsToKeep <- c("mmsi","timestamp","lon","lat","speed","heading","course")

##subset on vars
# fullKineticsTomodel <- fullKinetics[,names(fullKinetics) %in% varsToKeep,with=F]
# saveRDS(fullKineticsTomodel,"kineticsTofeatures.rds")

fullKineticsTomodel <- readRDS("kineticsTofeatures.rds")

###isolate trips to PIR

#keep only non NA observations
fullKineticsTomodel <- fullKineticsTomodel[complete.cases(fullKineticsTomodel)]

#quantiles to cut outliers
floorCutlon <- quantile(fullKineticsTomodel$lon,0.001)
cealingCutlon<- quantile(fullKineticsTomodel$lon,0.999)


floorCutlat <- quantile(fullKineticsTomodel$lat,0.001)
cealingCutlat<- quantile(fullKineticsTomodel$lat,0.999)


fullKineticsTomodel <- fullKineticsTomodel[lon > floorCutlon & lon < cealingCutlon]
fullKineticsTomodel <- fullKineticsTomodel[lat > floorCutlat & lat < cealingCutlat]

###add pireus flag 37.9405547,23.6245785
fullKineticsTomodel <- fullKineticsTomodel[,c("lonPIR","latPIR"):=.(23.62457, 37.94055)]
vessel <- unique(fullKineticsTomodel$mmsi)

### calculate distance of each geo point from PIR
cl<-makeCluster(detectCores())
registerDoParallel(cl)


distpar <- foreach (i = vessel) %dopar% distVessel(i)
dist_par_all <- rbindlist(distpar)

saveRDS(dist_par_all,"kineticsTofeatureswithDist.rds")


###count duration of each interobservation from PIR per trip

###build model frame

### candidaate models


