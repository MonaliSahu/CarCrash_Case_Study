# CarCrash_Case_Study
This case study intends to analyse the car crash data and present the findings for Vehicle Accidents across US. Below is the description of the Datasets used and the Analytics performed.

**DATASET**

For the analysis 6 datasets are used. The Data dictionary for all the Datasets are given below. 

![image](https://user-images.githubusercontent.com/48520317/216545649-aac13e20-3656-4236-8ed5-f5264e32bcaa.png)

![image](https://user-images.githubusercontent.com/48520317/216545714-3792c13a-ec94-4d96-aabf-4e9342694ef2.png)

![image](https://user-images.githubusercontent.com/48520317/216552293-f3f9c43d-d312-42fd-803c-76c637eb46d5.png)


**ANALYTICS**

Application performs below analysis and store the results for each analysis.
  1.	Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
  2.	Analysis 2: How many two wheelers are booked for crashes? 
  3.	Analysis 3: Which state has highest number of accidents in which females are involved? 
  4.	Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
  5.	Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
  6.	Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
  7.	Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
  8.	Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
  
**PROJECT STRUCTURE**

1. Source_Code: This folder contains all the source codes required to run the spark application with the utilities package.
2. Data.zip: Has sample input datasets
3. execution.bat: This file helps to run the spark application after the repo is cloned.

**REQUIREMENTS FOR THE APPLICATION**

1. Java installation & environment variable setup "JAVA_HOME" (https://www.oracle.com/in/java/technologies/javase/jdk11-archive-downloads.html)
2. Spark installation &  environment variable setup "SPARK_HOME" (https://www.apache.org/dyn/closer.lua/spark/spark-3.2.3/spark-3.2.3-bin-hadoop2.7.tgz)
3. Python3 installation & environment variable setup
4. Winutils.exe download and should be placed in C-drive hadoop/bin folder. Also set environment variable "HADOOP_HOME" (https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe)

**EXECUTION**
1. Clone the repository
2. In Command prompt, go to repo location i.e., "CarCrash_Case_study folder"
3. Type execution.bat 

NOTE:- execution.bat has all the commands required to run the spark application. In case of manual execution commands can be referred from here.

