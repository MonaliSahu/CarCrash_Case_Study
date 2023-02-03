# -*- coding: utf-8 -*-
"""
@author: Monali Sahu
"""

'''
DESCRIPTION-
    This is a part of utilities package and works as a helper code. 
    It has repeatable functions that are being referred read write operations.
'''

import json
from pyspark.sql import SparkSession

def initiate_sparkSession(appname):
    
    '''
    Initiates an object of sparkSession class.
    ARGUMENT:
        appname: application name used for referring the spark application.
    RETURNS: sparkSession object
    '''
    
    spark=SparkSession\
            .builder\
            .appName(appname)\
            .getOrCreate()
            
    spark.sparkContext.setLogLevel("ERROR")
            
    return spark

def read_config(config_file_path):
    
    '''
    Reads the configuration json file.
    ARGUMENT:
        config_file_path: location of the json file
    RETURNS: dictionary with the json data
    '''
    
    f=open(config_file_path)
    return json.load(f)

def read_csv(spark,input_file_path):
    
    '''
    Reads the csv files as DataFrames.
    ARGUMENT:
        spark:sparkSession object.
        input_file_path: location of the csv file
    RETURNS: dataframe
    '''
    
    df=spark\
        .read\
        .option('header',True)\
        .option('inferSchema',True)\
        .csv(input_file_path)
    
    return df

def save_output(df,output_file_path,output_file_format):
    
    '''
    Saves the dataframes in the given format.
    ARGUMENT:
        df: dataframe to be saved.
        output_file_path: location in which dataframe is saved
        output_file_format: the format in which dataframe should be saved (like csv, parqeut etc.)
    RETURNS: none
    '''
    
    df.coalesce(1)\
        .write\
        .format(output_file_format)\
        .mode("overwrite")\
        .option("header",True)\
        .save(output_file_path)

