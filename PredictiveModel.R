####Boat Arrival Prediction Model####

### load libs
library(data.table);library(h2o)
set.seed(80)
options(scipen=999)

### load helper user defined functions
source("/PiachaMSc/BoatArrivalPrediction/auxiliaryFunctions.R")
setwd("C:/PiachaMSc/BoatArrivalPrediction/kinematics/")


###read model data
###divide into train and test datasets

#start H2O on your local machine using all available cores
h2o.init(nthreads=-1) 

data <- readRDS("modelFrame.rds") 

# data[,timeToPort:= round(timeToPort/60)]

modelData <- as.h2o(data)


### split data into training test 
splits <- h2o.splitFrame(modelData, 0.90,seed = 33) 


#specify the parameters of the DL model
dl1<-h2o.deeplearning(x=1:5, y="timeToPort",
                      activation="Rectifier",
                      training_frame = splits[[1]], 
                      hidden=c(30,30,30),
                      standardize =T) 

pred1 <- h2o.predict(dl1,splits[[2]])

predFrame <- cbind(as.data.table(splits[[2]]),as.data.table(round(pred1)))

accuracy <- cor(predFrame$timeToPort,predFrame$predict)**2

predFrame[,timeToPort:=round(timeToPort/60)]
predFrame[,predict:=round(predict/60)]

