### load libs
library(data.table)

###list files
setwd("./kinematics/")
fileNames <- list.files(paste0(getwd()))

###get data
dataList <- lapply(fileNames, fread)

###unify list
longKinetics <- rbindlist(dataList)

###save to binary
saveRDS(longKinetics,"longKinetics.rds")
