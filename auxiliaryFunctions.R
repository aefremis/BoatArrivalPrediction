earth.dist <- function (long1, lat1, long2, lat2)
{
  rad <- pi/180
  a1 <- lat1 * rad
  a2 <- long1 * rad
  b1 <- lat2 * rad
  b2 <- long2 * rad
  dlon <- b2 - a2
  dlat <- b1 - a1
  a <- (sin(dlat/2))^2 + cos(a1) * cos(b1) * (sin(dlon/2))^2
  c <- 2 * atan2(sqrt(a), sqrt(1 - a))
  R <- 6378.145
  d <- R * c
  return(d)
}


distVessel <- function(Indvessel){
  library(data.table)
  vesselfullKineticsTomodel <- fullKineticsTomodel[mmsi==Indvessel]
  distList <- list()
  for (i in  1: nrow(vesselfullKineticsTomodel)){
    distList[[i]] <- earth.dist(vesselfullKineticsTomodel[i,]$lon,
                                vesselfullKineticsTomodel[i,]$lat,
                                vesselfullKineticsTomodel[i,]$lonPIR,
                                vesselfullKineticsTomodel[i,]$latPIR)
    print(paste0("prcComp: ",round(i/nrow(vesselfullKineticsTomodel)*100 ,3), " %"))
  }
  distVec <- unlist(distList)
  vesselfullKineticsTomodel[,distancePIR:=distVec]
  vesselfullKineticsTomodel
}


