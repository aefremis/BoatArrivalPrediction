### load libs
library(data.table);library(doParallel);library(foreach);library(zoo)
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

# fullKineticsTomodel <- readRDS("kineticsTofeatures.rds")

###isolate trips to PIR

#keep only non NA observations
# fullKineticsTomodel <- fullKineticsTomodel[complete.cases(fullKineticsTomodel)]

#quantiles to cut outliers
# floorCutlon <- quantile(fullKineticsTomodel$lon,0.001)
# cealingCutlon<- quantile(fullKineticsTomodel$lon,0.999)
# 
# 
# floorCutlat <- quantile(fullKineticsTomodel$lat,0.001)
# cealingCutlat<- quantile(fullKineticsTomodel$lat,0.999)
# 
# 
# fullKineticsTomodel <- fullKineticsTomodel[lon > floorCutlon & lon < cealingCutlon]
# fullKineticsTomodel <- fullKineticsTomodel[lat > floorCutlat & lat < cealingCutlat]

###add pireus flag 37.9405547,23.6245785
# fullKineticsTomodel <- fullKineticsTomodel[,c("lonPIR","latPIR"):=.(23.62457, 37.94055)]
# vessel <- unique(fullKineticsTomodel$mmsi)

### calculate distance of each geo point from PIR
# cl<-makeCluster(detectCores())
# registerDoParallel(cl)
# 
# 
# distpar <- foreach (i = vessel) %dopar% distVessel(i)
# dist_par_all <- rbindlist(distpar)

# saveRDS(dist_par_all,"kineticsTofeatureswithDist.rds")
dist_par_all <- readRDS("kineticsTofeatureswithDist.rds")
# stopCluster(cl)
###count duration of each interobservation from PIR per trip
#order by mmsi and timestamo

# kineticsToModel <- dist_par_all[order(mmsi,timestamp)]

# assume entrance in port in 800m
# kineticsToModel <- kineticsToModel[distancePIR>0.8]
# kineticsToModel <- kineticsToModel[,distancePIR:=round(distancePIR,1)]

#remove low observations vessels
# vesselsToCut <- kineticsToModel[,.N,by=mmsi][N<100]$mmsi
# kineticsToModel <- kineticsToModel[!mmsi %in% vesselsToCut]

#characterize trip as inbound or outbound

#calculate consecutive time differences
# kineticsToModel[,timeDistance:=timedistance(timestamp),by=mmsi]

#create lag of distance
# kineticsToModel[, lagDistancePort := shift(distancePIR,
#                                            1L,
#                                            fill=NA,
#                                            type="lag"),
#                 by=mmsi]

#calculate difference of lagged distance with distance
# kineticsToModel[,DifferenceLag:=lagDistancePort-distancePIR]
# kineticsToModel <- kineticsToModel[!DifferenceLag==0]


# find change in trend of distance
# kineticsToModel[,sign1:=sign(DifferenceLag)]
# 
# kineticsToModel[, sign2 := shift(sign1,
#                                            1L,
#                                            fill=NA,
#                                            type="lag"),
#                 by=mmsi]
# 
# kineticsToModel <- kineticsToModel[complete.cases(kineticsToModel)]
# kineticsToModel[,flagOfChange:=ifelse(sign1==sign2,0,1)]
# kineticsToModel <- kineticsToModel[! sign1==0 ]
# kineticsToModel <- kineticsToModel[! sign2==0 ]
# 
# 
# onlyChanges <- kineticsToModel[flagOfChange==1]
# onlyChanges[,tripID:=1:nrow(onlyChanges)]
# onlyChanges <- onlyChanges[,c("mmsi","timestamp","tripID")]
# 
# setkeyv(onlyChanges,c("mmsi","timestamp"))
# setkeyv(kineticsToModel,c("mmsi","timestamp"))


#unique tripID's
# fullTripInfo <- merge(kineticsToModel,onlyChanges,all.x = T)
# fullTripInfo[,tripID:=shift(tripID,1,fill=NA,type = "lead")]
# fullTripInfo[,tripID:=na.locf(tripID,fromLast = T,na.rm = T)]
# fullTripInfo <- fullTripInfo[!tripID==1]
# 
# fullTripInfo[,c("first","last"):=.(distancePIR[1],distancePIR[.N]),
#              by=.(tripID)][,direction:=first-last]
# 
# fullTripInfo[,tripType:=ifelse(direction<0,"outbound","inbound")]


# dataToModel <- fullTripInfo[tripType=="inbound"]
# saveRDS(dataToModel,"kineticsInbound.rds")
# kineticsInbound <- readRDS("kineticsInbound.rds")
# 
# varsToDrop <- c("lon","lat","lonPIR","latPIR","lagDistancePort","DifferenceLag","sign1","sign2",
#                 "flagOfChange","first","last","direction","tripType")
# kineticsToModel <- kineticsInbound[,!names(kineticsInbound) %in% varsToDrop,with=F]

# add time to PIR
# kineticsToModel[,timeToPort:=timestamp[.N]-timestamp,
#                 by=tripID]
# kineticsToModel[,timeToPort:=round(as.numeric(timeToPort),2)]


###build model frame
# varToModel <- c("speed","course","distancePIR","timeToPort")
# 
# modelFrame <- kineticsToModel[,names(kineticsToModel) %in% varToModel,with=F]
# lowerLimit <- quantile(modelFrame$timeToPort,0.02)
# upperLimit <- quantile(modelFrame$timeToPort,0.98)
# modelFrame <- modelFrame[timeToPort >=lowerLimit & timeToPort <= upperLimit]
# 
# saveRDS(modelFrame,"modelFrame.rds")

