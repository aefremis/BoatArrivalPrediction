####Data Engineering for Boat Arrival Prediction Model####

### load libs
library(data.table);library(doParallel);library(foreach);library(zoo);library(devtools)

### load helper user defined functions
source("/PiachaMSc/BoatArrivalPrediction/auxiliaryFunctions.R")
options(scipen=999)

###list files
# setwd("./kinematics/")

###get names of kinetics files
fileNames <- list.files(paste0(getwd()))

###load ship id
shipID <- fread("ship_id.csv")

###isolate passenger ships
shipID_passenger <- shipID[type %like% "Passenger" | type %like% "passenger"]


###get data by name to a list
dataList <- lapply(fileNames, fread)

###bind list into a data.table
longKinetics <- rbindlist(dataList)

###save indermediate file
saveRDS(longKinetics,"longKinetics.rds")

###load intermidiate file
system.time(longKinetics <- readRDS("longKinetics.rds"))

###verify all ais type are position reports 
table(longKinetics$type)
# 1        2        3       18       19 
# 60328736   486998  8337485   645770    10318 
### drop type variable
longKinetics[,type:=NULL]

###keep only boats with status=0 "under way using engine"
longKinetics <- longKinetics[status==0]

### set primary key for shipID
setkeyv(shipID_passenger,"mmsi")

###set primary key for kinetics
setkeyv(longKinetics,"mmsi")

###merge  shipid_passenger with kinetics 
fullKinetics <- merge(longKinetics,shipID_passenger)


###turn UNIX timestamps into datetime R types 
fullKinetics[,timestamp:=as.POSIXct(as.numeric(as.character(timestamp)),
                                               origin="1970-01-01",
                                               tz="Europe/Athens")]


###delete initial kinetics table for memory benefit
rm(longKinetics)

###keep only relevan variables
varsToKeep <- c("mmsi","timestamp","lon","lat","speed","heading","course")

###subset relevant variables 
fullKineticsTomodel <- fullKinetics[,names(fullKinetics) %in% varsToKeep,
                                    with=F]

###save intermediate file
saveRDS(fullKineticsTomodel,"kineticsTofeatures.rds")

###read intermediate file
fullKineticsTomodel <- readRDS("kineticsTofeatures.rds")

###keep only non NA observations
fullKineticsTomodel <- fullKineticsTomodel[complete.cases(fullKineticsTomodel)]

### clean outliers in lon lat

floorCutlon <- quantile(fullKineticsTomodel$lon,0.001)
cealingCutlon<- quantile(fullKineticsTomodel$lon,0.999)

floorCutlat <- quantile(fullKineticsTomodel$lat,0.001)
cealingCutlat<- quantile(fullKineticsTomodel$lat,0.999)

fullKineticsTomodel <- fullKineticsTomodel[lon > floorCutlon & lon < cealingCutlon]
fullKineticsTomodel <- fullKineticsTomodel[lat > floorCutlat & lat < cealingCutlat]

### add pireus flag 37.9405547,23.6245785
fullKineticsTomodel <- fullKineticsTomodel[,c("lonPIR","latPIR"):=.(23.62457, 37.94055)]


### calculate distance of each geo point from PIR

###build distibuted process
vessel <- unique(fullKineticsTomodel$mmsi)
cl<-makeCluster(detectCores())
registerDoParallel(cl)

distpar <- foreach (i = vessel) %dopar% distVessel(i)
dist_par_all <- rbindlist(distpar)
stopCluster(cl)

### save intermediate file
saveRDS(dist_par_all,"kineticsTofeatureswithDist.rds")

###load intermediate file
dist_par_all <- readRDS("kineticsTofeatureswithDist.rds")

###order by mmsi and timestamp

kineticsToModel <- dist_par_all[order(mmsi,timestamp)]

### assume entrance in port in 800m
kineticsToModel <- kineticsToModel[distancePIR>0.8]
kineticsToModel <- kineticsToModel[,distancePIR:=round(distancePIR,1)]

##remove low observations vessels
vesselsToCut <- kineticsToModel[,.N,by=mmsi][N<100]$mmsi
kineticsToModel <- kineticsToModel[!mmsi %in% vesselsToCut]

###characterize trip as inbound or outbound

###create lag of distance [lag=1]
kineticsToModel[, lagDistancePort := shift(distancePIR,
                                           1L,
                                           fill=NA,
                                           type="lag"),
                by=mmsi]

###calculate difference of lagged distance with distance
kineticsToModel[,DifferenceLag:=lagDistancePort-distancePIR]
kineticsToModel <- kineticsToModel[!DifferenceLag==0]


###find change in trend of distance
kineticsToModel[,sign1:=sign(DifferenceLag)]
 
kineticsToModel[, sign2 := shift(sign1,
                                           1L,
                                           fill=NA,
                                           type="lag"),
                by=mmsi]
 
kineticsToModel <- kineticsToModel[complete.cases(kineticsToModel)]

###flag change of trip
kineticsToModel[,flagOfChange:=ifelse(sign1==sign2,0,1)]

###remove time points were boat is inactive
kineticsToModel <- kineticsToModel[! sign1==0 ]
kineticsToModel <- kineticsToModel[! sign2==0 ]
 

###create a table only with points of change 
onlyChanges <- kineticsToModel[flagOfChange==1]
onlyChanges[,tripID:=1:nrow(onlyChanges)]
onlyChanges <- onlyChanges[,c("mmsi","timestamp","tripID")]

###set primary keys
setkeyv(onlyChanges,c("mmsi","timestamp"))
setkeyv(kineticsToModel,c("mmsi","timestamp"))


### create unique tripID's
fullTripInfo <- merge(kineticsToModel,onlyChanges,all.x = T)

###shift to match point of new trip
fullTripInfo[,tripID:=shift(tripID,1,fill=NA,type = "lead")]

###fill NA's over each number wuth that number[tripID]
fullTripInfo[,tripID:=na.locf(tripID,fromLast = T,na.rm = T)]

###remove repetitive tripID==1
fullTripInfo <- fullTripInfo[!tripID==1]

###check if the vessel is moving towards or away from PIR
fullTripInfo[,c("first","last"):=.(distancePIR[1],distancePIR[.N]),
             by=.(tripID)][,direction:=first-last]

###characterize trip as inbound ori outbound
fullTripInfo[,tripType:=ifelse(direction<0,"outbound","inbound")]


###keep only inbound trips
dataToModel <- fullTripInfo[tripType=="inbound"]

###save intermediate file
saveRDS(dataToModel,"kineticsInbound.rds")
kineticsInbound <- readRDS("kineticsInbound.rds")

###remove variables we dont need anymore 
varsToDrop <- c("lonPIR","latPIR","lagDistancePort","DifferenceLag","sign1","sign2",
                "flagOfChange","first","last","direction","tripType")
kineticsToModel <- kineticsInbound[,!names(kineticsInbound) %in% varsToDrop,with=F]

###calculate for each observation the time to PIR
kineticsToModel[,timeToPort:=timestamp[.N]-timestamp,
                by=tripID]
kineticsToModel[,timeToPort:=round(as.numeric(timeToPort),2)]


###build model frame
varToModel <- c("lon","lat","speed","course","distancePIR","timeToPort")
modelFrame <- kineticsToModel[,names(kineticsToModel) %in% varToModel,with=F]

###remove outliers 
lowerLimit <- quantile(modelFrame$timeToPort,0.02)
upperLimit <- quantile(modelFrame$timeToPort,0.98)
modelFrame <- modelFrame[timeToPort >=lowerLimit & timeToPort <= upperLimit]
 

###save data to model
saveRDS(modelFrame,"modelFrame.rds")