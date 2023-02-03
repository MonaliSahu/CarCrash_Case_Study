# -*- coding: utf-8 -*-
"""
@author: Monali Sahu
"""

from utilities.utils import read_csv, save_output
from pyspark.sql.functions import sum,count,col,row_number,rank,countDistinct
from pyspark.sql.window import Window


class Analyzer:
    
    '''
    DESCRIPTION-
        This is a class that defines all the transformations & actions in order to solve the problem statements.
    CLASS METHODS: preview_dataset(),performAnalysis()
    CLASS VARIABLES: spark, unit, charges, damages, restrict, endorse, primary_person, output_file_path, output_file_format, unit_joined_pp
    '''
    
    #initiating all class variables
    def __init__(self,spark,input_file_path,output_file_path,output_file_format):
        
        '''
        This is the constructor of the class.
        ARGUMENTS:
            spark: sparkSession object
            input_file_path: dictionary with dataset names as key and paths for each as value
            output_file_path: dictionary with analysis<n> as key and paths for each as value
            output_file_format: the format in which all analysis outputs will be saved
        '''
        
        self.spark=spark;
        
        #reading the datasets
        self.unit=read_csv(self.spark,input_file_path['Unit']).distinct()
        self.charges=read_csv(self.spark,input_file_path['Charges']).distinct()
        self.damages=read_csv(self.spark,input_file_path['Damages']).distinct()
        self.restrict=read_csv(self.spark,input_file_path['Restrict']).distinct()
        self.endorse=read_csv(self.spark,input_file_path['Endorsements']).distinct()
        self.primary_person=read_csv(self.spark,input_file_path['Primary_Person']).distinct()
        
        self.output_file_path=output_file_path
        self.output_file_format=output_file_format
        
        #creating joined dataset
        self.unit_joined_pp=self.unit.join(self.primary_person, on=["CRASH_ID","UNIT_NBR"],how="inner")
        
        
    def preview_dataset(self):
        
        '''
        Function that presents data samples of each dataset
        ARGUMENTS: none
        RETURNS: none
        '''
        
        print("DATASET: Unit")
        self.unit.show(5)
        print("DATASET: Charges")
        self.charges.show(5)
        print("DATASET: Damages")
        self.damages.show(5)
        print("DATASET: Restrict")
        self.restrict.show(5)
        print("DATASET: Endorse")
        self.endorse.show(5)
        print("DATASET: Primary Person")
        self.primary_person.show(5)
        
    
    def performAnalysis(self,analysis_number):
        
        '''
        Function that performs the analysis as per problem statement
        ARGUMENTS: 
            analysis_number: A number stating which Analysis has to be done (1,2,3....8)
        RETURNS: value or dataframe based on the problem statement
        '''
        
        if analysis_number==1:
            
            '''
            Find the number of crashes (accidents) in which number of persons killed are male?
            DATASET USED:- Primary_Person
            '''
            
            analysis=self.primary_person\
                        .filter((self.primary_person.PRSN_GNDR_ID == "MALE") & (self.primary_person.DEATH_CNT > 0))\
                        .groupBy('CRASH_ID')\
                        .agg(sum("DEATH_CNT")\
                        .alias("Total_male_deaths"))
                            
            output=analysis.count()
            
            output_file_path=self.output_file_path['Analysis1']
        
        elif analysis_number==2:
            
            '''
            How many two wheelers are booked for crashes?
            DATASETS USED:- Unit
            '''
            
            analysis=self.unit.filter(self.unit.VEH_BODY_STYL_ID.contains('MOTORCYCLE'))\
                        .select('CRASH_ID','UNIT_DESC_ID','VIN')\
                        .distinct()
                        
            output=analysis.count()
            
            output_file_path=self.output_file_path['Analysis2']
                        
        elif analysis_number==3:
            
            '''
            Which state has highest number of accidents in which females are involved? 
            DATASETS USED:- Primary_Person
            '''
            
            analysis=self.primary_person\
                        .filter((self.primary_person.PRSN_GNDR_ID == 'FEMALE') & (~self.primary_person.DRVR_LIC_STATE_ID.isin('NA','Unknown','Other')))\
                        .groupBy('DRVR_LIC_STATE_ID')\
                        .count()\
                        .orderBy(col("count").desc())
                        
            output=analysis.first().DRVR_LIC_STATE_ID
            
            output_file_path=self.output_file_path['Analysis3']
            
            
        elif analysis_number==4:
            
            '''
            Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death?
            DATASETS USED:- Unit
            '''
            
            analysis=self.unit.filter(self.unit.VEH_MAKE_ID !='NA')\
                        .withColumn("TOTAL_INJURED",self.unit.TOT_INJRY_CNT+self.unit.DEATH_CNT)\
                        .groupBy('VEH_MAKE_ID')\
                        .agg(sum("TOTAL_INJURED").alias("TOTAL_INJURED_INC_DEATH"))
            
            selection_window=Window.orderBy(col("TOTAL_INJURED_INC_DEATH").desc())             
            
            analysis=analysis\
                    .withColumn("row_number",row_number().over(selection_window))\
                    .filter((col("row_number")>4) & (col("row_number")<16))\
                    .select("VEH_MAKE_ID","TOTAL_INJURED_INC_DEATH")
                        
            output=analysis.select("VEH_MAKE_ID")
            
            output_file_path=self.output_file_path['Analysis4']
            
        elif analysis_number==5:
            
            '''
            For all the body styles involved in crashes, mention the top ethnic user group of each unique body style.
            DATASETS USED:- Primary_Person, Unit
            '''
            
            analysis=self.unit_joined_pp\
                  .filter((~self.unit_joined_pp.PRSN_ETHNICITY_ID.isin('NA','UNKNOWN','OTHER')) & (~self.unit_joined_pp.VEH_BODY_STYL_ID.isin("NA","UNKNOWN","NOT REPORTED","OTHER  (EXPLAIN IN NARRATIVE)")))\
                  .groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID')\
                  .agg(count(self.unit_joined_pp.CRASH_ID).alias("TOTAL_CRASHES"))
                  
            rank_window=Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('TOTAL_CRASHES').desc())
            
            analysis=analysis.withColumn("rank",rank().over(rank_window))\
                    .filter(col("rank")==1)\
                    .select('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID')
            
            output=analysis
            
            output_file_path=self.output_file_path['Analysis5']
            
        elif analysis_number==6:
            
            '''
            Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)?
            DATASETS USED:- Primary_Person, Unit
            '''
            
            analysis=self.unit_joined_pp\
                        .filter(self.unit_joined_pp.DRVR_ZIP!='null')\
                        .filter(self.unit_joined_pp.VEH_BODY_STYL_ID.contains('CAR'))\
                        .filter((self.unit_joined_pp.CONTRIB_FACTR_2_ID.contains('ALCOHOL'))|(self.unit_joined_pp.CONTRIB_FACTR_1_ID.contains('ALCOHOL')))\
                        .groupBy('DRVR_ZIP')\
                        .agg(countDistinct(self.unit_joined_pp.CRASH_ID).alias("TOTAL_CRASHES"))\
                        .orderBy(col('TOTAL_CRASHES').desc()).limit(5)
                        
            output=analysis.select('DRVR_ZIP')
            
            output_file_path=self.output_file_path['Analysis6']
            
        elif analysis_number==7:
            
            '''
            Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.
            DATASETS USED:- Unit, Damages, 
            '''
            
            analysis=self.unit.join(self.damages, on=['CRASH_ID'],how='outer')\
                            .filter((self.damages.DAMAGED_PROPERTY=="NONE")|(self.damages.DAMAGED_PROPERTY=="")|(self.damages.DAMAGED_PROPERTY.isNull()))\
                            .filter(((~self.unit.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])) 
                                    & (self.unit.VEH_DMAG_SCL_1_ID >'DAMAGED 4')) | 
                                    ((~self.unit.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])) 
                                            & (self.unit.VEH_DMAG_SCL_2_ID >'DAMAGED 4')))\
                             .filter(self.unit.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")\
                             .select("CRASH_ID","VEH_DMAG_SCL_1_ID","VEH_DMAG_SCL_2_ID")
                              
            output=analysis.select("CRASH_ID").distinct().count()

            output_file_path=self.output_file_path['Analysis7']

        else:
            
            '''
            Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, 
            used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
            DATASETS USED:- Primary_Person, Unit, Charges
            '''

            highest_offence_States=self.unit\
                                        .groupby("VEH_LIC_STATE_ID").count()\
                                        .orderBy(col("count").desc()).limit(25)\
                                        .select("VEH_LIC_STATE_ID")
            
            top_vehicle_colors=self.unit.filter(self.unit.VEH_COLOR_ID != "NA")\
                                        .groupby("VEH_COLOR_ID").count()\
                                        .orderBy(col("count").desc()).limit(10)\
                                        .select("VEH_COLOR_ID")

            analysis=self.unit_joined_pp.join(self.charges, on=["CRASH_ID","UNIT_NBR"],how="inner")\
                        .join(highest_offence_States,on=["VEH_LIC_STATE_ID"],how="inner")\
                        .join(top_vehicle_colors,on=["VEH_COLOR_ID"],how="inner")\
                        .filter(self.charges.CHARGE.contains("SPEED"))\
                        .filter(self.primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))\
                        .groupby("VEH_MAKE_ID").agg(count(self.charges.CRASH_ID).alias("TOTAL_CHARGES"))\
                        .orderBy(col("TOTAL_CHARGES").desc()).limit(5)
                        
            output=analysis.select("VEH_MAKE_ID") 
            
            output_file_path=self.output_file_path['Analysis8']
                        
        #saving the output through save_ourtput() of utils class
        save_output(analysis,output_file_path, self.output_file_format)
        
        #returning the output back to the function call.
        return output