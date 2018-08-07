####Boat Arrival Prediction Model####

### load libs
library(data.table);library(h2o)
set.seed(80)
options(scipen=999)

### load helper user defined functions
setwd("C:/PiachaMSc/BoatArrivalPrediction/kinematics/")


###read model data
###divide into train and test datasets

#start H2O on your local machine using all available cores
h2o.init(nthreads=-1) 

data <- readRDS("modelFrame.rds") 

modelData <- as.h2o(data)

### split data into training test 
splits <- h2o.splitFrame(modelData, 0.8) 


#specify the parameters of the DL model
predictiveModel_NN<-h2o.deeplearning(x=1:5, y="timeToPort",
                      activation="Rectifier",
                      training_frame = splits[[1]], 
                      hidden=c(30,30,30),
                      standardize =T) 

predictions <- h2o.predict(predictiveModel_NN,splits[[2]])

predFrame <- cbind(as.data.table(splits[[2]]),as.data.table(round(predictions)))
predFrame[,MAE:=mean((abs(timeToPort-predict)))] 
# calculation of R-squared
predFrame[,accuracy:=cor(predFrame$timeToPort,predFrame$predict)**2] 

#save models
saveRDS(predictiveModel_NN,"DNN_Boat.rds")



###linear regression

linmodel <- lm(timeToPort~.,data = data)
summary(linmodel)
library(car)
vif(linmodel)
plot(density(resid(linmodel)))

               
               