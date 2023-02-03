# -*- coding: utf-8 -*-
"""
@author: Monali Sahu
"""

'''
DESCRIPTION-
    This is the main program that runs the whole Car Crash Case Study.
'''

import sys
from sourceCodes.utilities import utils,configChecks
from sourceCodes.carCrashAnalysis import Analyzer
import logging


def main(config_file_path):
    
    logging.basicConfig(filename='car_crash.log',format='%(asctime)s %(levelname)s: %(message)s',level=logging.INFO, datefmt='%d/%m/%Y %I:%M:%S')

    
    print("BCG Car Crash Analysis")
    logging.info('----- STARTED BCG Car Crash Analysis Spark Application -----')
    
    #Reading config file
    try:
        
        config=utils.read_config(config_file_path)
        print("SUCCESS: Configuration file loaded")
        logging.info('Configuration file loaded')
        
    except Exception as e:
        
        print("FAILURE: Terminating Application, Unable to read config file")
        print("REASON: ", e)
        logging.error("Terminating Application, Unable to read config file")
        sys.exit(1)
    
    #Fetching App name : if not available assigning default value
    app_name=config.get('APP_NAME',"BCG_Car_Crash_Analysis")
    
    #Initiating Spark Session
    try:
        
        spark=utils.initiate_sparkSession(app_name)
        print("SUCCESS: Spark Session initiated for app - ",app_name)
        logging.info("Spark Session initiated for app - "+app_name)
        
    except Exception as e:
        
        print("FAILURE: Terminating Application, Unable to initiate spark session")
        print("REASON: ", e)
        logging.error("Terminating Application, Unable to initiate spark session")
        sys.exit(1)
        
    #Data Checks
    
    if 'INPUT_FILE_PATHS' in config.keys():
        status=configChecks.input_check(config['INPUT_FILE_PATHS'])
    
        if status:
            print("SUCCESS: All Input files are present. Proceeding Ahead.")
            logging.info("All Input files are present.")
            
        else:
            print("FAILURE: Terminating Application, Not All the input files are available.")
            logging.error("Terminating Application, Not All the input files are available.")
            sys.exit(1)
            
    else:
        print("FAILURE: No INPUT FILE PATHS provided in Config file.")
        logging.error("No INPUT FILE PATHS provided in Config file.")
        
    #Output Location Value check: Whether provided for all the Analysis or not.
    
    if 'OUTPUT_FILE_PATHS' in config.keys():
        status=configChecks.output_check(config['OUTPUT_FILE_PATHS'])
    
        if status:
            print("SUCCESS: All output paths are mentioned. Proceeding Ahead.")
            logging.info("All output paths are mentioned.")
            
        else:
            print("FAILURE: Not All the output paths are available.")
            logging.warning("No INPUT FILE PATHS provided in Config file.")
            
            print("ASSIGNING Default Location 'pwd/Output/Analysis<n>' to all")
            logging.info("ASSIGNING Default Location 'pwd/Output/Analysis<n>' to all")
            
            config['OUTPUT_FILE_PATHS']={
               "Analysis1":"Output/Analysis1",
               "Analysis2":"Output/Analysis2",
               "Analysis3":"Output/Analysis3",
               "Analysis4":"Output/Analysis4",
               "Analysis5":"Output/Analysis5",
               "Analysis6":"Output/Analysis6",
               "Analysis7":"Output/Analysis7",
               "Analysis8":"Output/Analysis8"
               }
            
    else:
        print("FAILURE: No OUTPUT FILE PATHS provided in Config file.")
        logging.warning("No OUTPUT FILE PATHS provided in Config file.")
        
        print("ASSIGNING OUTPUT FILE PATHS to Default Location 'pwd/Output/Analysis<n>' to all")
        logging.info("ASSIGNING Default Location 'pwd/Output/Analysis<n>' to all")
        
        config['OUTPUT_FILE_PATHS']={
           "Analysis1":"Output/Analysis1",
           "Analysis2":"Output/Analysis2",
           "Analysis3":"Output/Analysis3",
           "Analysis4":"Output/Analysis4",
           "Analysis5":"Output/Analysis5",
           "Analysis6":"Output/Analysis6",
           "Analysis7":"Output/Analysis7",
           "Analysis8":"Output/Analysis8"
           }
    
    #fetching output file format: if not available assigning default format csv 
    output_file_format=config.get('OUTPUT_FILE_FORMAT','csv')
    
    #Creating Analyzer object
    analyzer_obj=Analyzer(spark,config['INPUT_FILE_PATHS'],config['OUTPUT_FILE_PATHS'],output_file_format)
    print("Car Crash Data Analyzer Initiated",end="\n")
    logging.info("Car Crash Data Analyzer Initiated")
    
    #printing sample data from each dataset
    print("----- DATASET Preview -----")
    analyzer_obj.preview_dataset()
    
    #Analysis 1
    op=analyzer_obj.performAnalysis(1)
    print("ANALYSIS 1: Total CRASHES with male deaths - ",op)
    logging.info("ANALYSIS 1: Total CRASHES with male deaths - "+str(op))
    
    #Analysis 2
    op=analyzer_obj.performAnalysis(2)
    print("ANALYSIS 2: Total 2-Wheelers booked for crash - ",op)
    logging.info("ANALYSIS 2: Total 2-Wheelers booked for crash -"+str(op))
    
    #Analysis 3
    op=analyzer_obj.performAnalysis(3)
    print("ANALYSIS 3: State with highest number of accidents involving females - ",op)
    logging.info("ANALYSIS 3: State with highest number of accidents involving females - "+str(op))
    
    #Analysis 4
    op=analyzer_obj.performAnalysis(4)
    print("ANALYSIS 4: Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death are - ")
    op.show(truncate=False)
    logging.info("ANALYSIS 4: Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death are - {}".format(op.rdd.flatMap(lambda x: x).collect()))
    
    #Analysis 5
    op=analyzer_obj.performAnalysis(5)
    print("ANALYSIS 5: For all the body styles involved in crashes,the top ethnic user group of each unique body style is shown below - ")
    op.show(truncate=False)    
    logging.info("ANALYSIS 5: For all the body styles involved in crashes,the top ethnic user group of each unique body style is shown below - {}".format(op.rdd.collect()))
    
    #Analysis 6
    op=analyzer_obj.performAnalysis(6)
    print("ANALYSIS 6: Among the crashed cars, the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash are - ")
    op.show(truncate=False)
    logging.info("ANALYSIS 6: Among the crashed cars, the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash are - {}".format(op.rdd.flatMap(lambda x: x).collect()))
    
    #Analysis 7
    op=analyzer_obj.performAnalysis(7)
    print("ANALYSIS 7: Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance - ",op)
    logging.info("ANALYSIS 7: Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance - "+str(op))
    
    #Analysis 8
    op=analyzer_obj.performAnalysis(8)
    print("ANALYSIS 8: Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences  - ")
    op.show(truncate=False)
    logging.info("ANALYSIS 8: Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences  - {}".format(op.rdd.flatMap(lambda x: x).collect()))

    print("----- PROCESS COMPLETED -----")
    logging.info("PROCESS COMPLETED")
    logging.shutdown()

if __name__=="__main__":
    
    config_file_path=sys.argv[1]
    #config_file_path="D:/CarCrash/configuration/config.json"
    main(config_file_path)