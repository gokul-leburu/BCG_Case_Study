# Databricks notebook source
"""
    Analysis is a class for performing various data analyses on vehicle crash data using Spark DataFrames.

    Attributes:
        input_config (dict): Configuration dictionary for input data paths.
        output_config (dict): Configuration dictionary for output data paths.
        functions (dict): Dictionary mapping analysis names to their respective methods.
        df_Units (DataFrame): DataFrame for the 'Units' CSV file with duplicates removed.
        df_Primary_Person (DataFrame): DataFrame for the 'Primary Person' CSV file.
        df_Charges (DataFrame): DataFrame for the 'Charges' CSV file with duplicates removed.
        df_Damages (DataFrame): DataFrame for the 'Damages' CSV file with duplicates removed.
        df_Endorse (DataFrame): DataFrame for the 'Endorse' CSV file.
        df_Restrict (DataFrame): DataFrame for the 'Restrict' CSV file.

    Methods:
        __init__(udf_csv, log_process, input_config, output_config):
            Initializes the Analysis class with configurations and loads necessary CSV files.

        Analysis_1(log_process, batch_id, V_PARENT_PROCESS, V_STAGE, V_PROCESS):
            Performs analysis to count crashes with more than two males killed.

        Analysis_2(log_process, batch_id, V_PARENT_PROCESS, V_STAGE, V_PROCESS):
            Performs analysis to count crashes involving motorcycles.

        Analysis_3(log_process, batch_id, V_PARENT_PROCESS, V_STAGE, V_PROCESS):
            Identify the top 5 vehicle makes involved in crashes where the driver was killed and the airbag did not deploy.

        Analysis_4(log_process, batch_id, V_PARENT_PROCESS, V_STAGE, V_PROCESS):
            Count the number of crashes involving vehicles involued in HIT and RUN and drivers with specific license types. 

        Analysis_5(log_process, batch_id, V_PARENT_PROCESS, V_STAGE, V_PROCESS):
            Identify the state with the highest number of accidents not involving female drivers with known license states. 

        Analysis_6(log_process, batch_id, V_PARENT_PROCESS, V_STAGE, V_PROCESS):
            Identify the vehicle makes ranked 3rd to 5th in terms of the total number of injuries including deaths. 

        Analysis_7(log_process, batch_id, V_PARENT_PROCESS, V_STAGE, V_PROCESS):
            Identifies the most common ethnicity for each vehicle body style involved in crashes. 

        Analysis_8(log_process, batch_id, V_PARENT_PROCESS, V_STAGE, V_PROCESS):
            Identify the top 5 driver ZIP codes associated with crashes where the driver was under the influence of alcohol.

        Analysis_9(log_process, batch_id, V_PARENT_PROCESS, V_STAGE, V_PROCESS):
            Count the number of crashes involving vehicles with no significant damage, and proof of liability insurance. 

        Analysis_10(log_process, batch_id, V_PARENT_PROCESS, V_STAGE, V_PROCESS):
            Identify the top 5 vehicle makes involved in speeding-related crashes, where the driver has a commercial or regular driver's license, and the vehicle is among the top 25 states and top 10 colors.
    """
class Analysis:
    def __init__(self,udf_csv,log_process,input_config,output_config):
        self.input_config=input_config
        self.output_config=output_config

        self.functions = {"Analysis_1":self.Analysis_1,"Analysis_2":self.Analysis_2,"Analysis_3":self.Analysis_3,"Analysis_4":self.Analysis_4,"Analysis_5":self.Analysis_5,"Analysis_6":self.Analysis_6,"Analysis_7":self.Analysis_7,"Analysis_8":self.Analysis_8,"Analysis_9":self.Analysis_9,"Analysis_10":self.Analysis_10}

        #found duplicates in df_units, so removing them
        self.df_Units= udf_csv.get_csv("Units").dropDuplicates().alias("unt")
        self.df_Primary_Person = udf_csv.get_csv("Primary Person").alias("pp")
        #found duplicates in df_Charges, so removing them
        self.df_Charges = udf_csv.get_csv("Charges").dropDuplicates().alias("chr")
        #found duplicates in df_Damages, so removing them
        self.df_Damages = udf_csv.get_csv("Damages").dropDuplicates().alias("dmg")
        self.df_Endorse = udf_csv.get_csv("Endorse").alias("end")
        self.df_Restrict = udf_csv.get_csv("Restrict").alias("rstr")

    def Analysis_1(self,log_process,batch_id,V_PARENT_PROCESS,V_STAGE,V_PROCESS):
        df_analysis_1 = self.df_Primary_Person.filter("pp.PRSN_GNDR_ID == 'MALE' and pp.PRSN_INJRY_SEV_ID='KILLED'")\
                                              .groupBy("CRASH_ID")\
                                              .agg(count("*").alias("No_of_male_killed"))\
                                              .filter("No_of_male_killed>2")\
                                              .agg(count("*").alias("Total_No_of_crash_id_analysis_1"))\
                                              .select("Total_No_of_crash_id_analysis_1")
        dbutils.fs.rm(self.output_config[V_PROCESS],True)
        df_analysis_1.write.format("csv").save(self.output_config[V_PROCESS])

    def Analysis_2(self,log_process,batch_id,V_PARENT_PROCESS,V_STAGE,V_PROCESS):
        df_analysis_2 = self.df_Units.filter("veh_body_styl_id ='MOTORCYCLE'")\
                                     .agg(count("*").alias("Total_No_of_two_wheeler_crash_analysis_1"))\
                                     .select("Total_No_of_two_wheeler_crash_analysis_1")
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_2.write.format("csv").save(output_config[V_PROCESS])

    def Analysis_3(self,log_process,batch_id,V_PARENT_PROCESS,V_STAGE,V_PROCESS):
        df_analysis_3 = self.df_Units.join(self.df_Primary_Person,(self.df_Primary_Person["CRASH_ID"]==self.df_Units["CRASH_ID"]) & (self.df_Primary_Person["UNIT_NBR"]==self.df_Units["UNIT_NBR"]))\
                        .filter("PRSN_INJRY_SEV_ID = 'KILLED' AND PRSN_AIRBAG_ID = 'NOT DEPLOYED' AND VEH_MAKE_ID!='NA'")\
                        .groupBy("VEH_MAKE_ID")\
                        .agg(count("*").alias("No_of_veh_per_brand"))\
                        .orderBy(desc("No_of_veh_per_brand"))\
                        .limit(5)
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_3.write.format("csv").save(output_config[V_PROCESS])

    def Analysis_4(self,log_process,batch_id,V_PARENT_PROCESS,V_STAGE,V_PROCESS):
        df_analysis_4 = self.df_Units.join(self.df_Primary_Person,(self.df_Primary_Person["CRASH_ID"]==self.df_Units["CRASH_ID"]) & (self.df_Primary_Person["UNIT_NBR"]==self.df_Units["UNIT_NBR"]))\
                               .filter("VEH_HNR_FL='Y' AND DRVR_LIC_TYPE_ID IN ('COMMERCIAL DRIVER LIC.' , 'DRIVER LICENSE')")\
                               .agg(count("*").alias("count"))
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_4.write.format("csv").save(output_config[V_PROCESS])

    def Analysis_5(self,log_process,batch_id,V_PARENT_PROCESS,V_STAGE,V_PROCESS):
        df_analysis_5 = self.df_Primary_Person.filter("PRSN_GNDR_ID !='FEMALE' and DRVR_LIC_STATE_ID not in ('NA','Unknown')")\
                                 .groupBy("DRVR_LIC_STATE_ID")\
                                 .agg(count("*").alias("Total_no_of_accidents"))\
                                 .orderBy(desc("Total_no_of_accidents"))\
                                 .limit(1)
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_5.write.format("csv").save(output_config[V_PROCESS])
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")

    def Analysis_6(self,log_process,batch_id,V_PARENT_PROCESS,V_STAGE,V_PROCESS):
        window_spec = Window.orderBy(desc("Total_no_of_injurys_and_deaths"))
        df_analysis_6= self.df_Units.filter("veh_make_id not in ('NA','UNKNOWN')")\
                       .groupBy("veh_make_id")\
                       .agg((sum("DEATH_CNT")+sum("TOT_INJRY_CNT")).alias("Total_no_of_injurys_and_deaths"))\
                       .withColumn("rk",row_number().over(window_spec))\
                       .filter("rk between 3 and 5")\
                       .select("veh_make_id")
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_6.write.format("csv").save(output_config[V_PROCESS])

    def Analysis_7(self,log_process,batch_id,V_PARENT_PROCESS,V_STAGE,V_PROCESS):
        window_spec = Window.partitionBy("veh_body_styl_id").orderBy(desc("Total_count"))
        df_analysis_7 = self.df_Units.join(self.df_Primary_Person,(self.df_Primary_Person["CRASH_ID"]==self.df_Units["CRASH_ID"]) & (self.df_Primary_Person["UNIT_NBR"]==self.df_Units["UNIT_NBR"]))\
                        .filter("veh_body_styl_id not in ('NA','UNKNOWN','OTHER  (EXPLAIN IN NARRATIVE)','NOT REPORTED') and PRSN_ETHNICITY_ID not in ('NA','UNKNOWN')")\
                        .groupBy("veh_body_styl_id","PRSN_ETHNICITY_ID")\
                        .agg(count("*").alias("Total_count"))\
                        .withColumn("rn",row_number().over(window_spec))\
                        .orderBy("veh_body_styl_id",desc("Total_count"),"PRSN_ETHNICITY_ID")\
                        .filter("rn=1")\
                        .select("veh_body_styl_id","PRSN_ETHNICITY_ID","Total_count")
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_7.write.format("csv").save(output_config[V_PROCESS])

    def Analysis_8(self,log_process,batch_id,V_PARENT_PROCESS,V_STAGE,V_PROCESS):

        df_analysis_8 = self.df_Units.join(self.df_Primary_Person,(self.df_Primary_Person["CRASH_ID"]==self.df_Units["CRASH_ID"]) & (self.df_Primary_Person["UNIT_NBR"]==self.df_Units["UNIT_NBR"]))\
                        .filter("(CONTRIB_FACTR_1_ID in ('HAD BEEN DRINKING','UNDER INFLUENCE - ALCOHOL') or CONTRIB_FACTR_2_ID in ('UNDER INFLUENCE - ALCOHOL','HAD BEEN DRINKING') or CONTRIB_FACTR_P1_ID in ('HAD BEEN DRINKING','UNDER INFLUENCE - ALCOHOL')) and DRVR_ZIP is not null")\
                        .groupBy("DRVR_ZIP")\
                        .agg(count("*").alias("Total_count"))\
                        .orderBy(desc("Total_count"))\
                        .limit(5)
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_8.write.format("csv").save(output_config[V_PROCESS])
    
    def Analysis_9(self,log_process,batch_id,V_PARENT_PROCESS,V_STAGE,V_PROCESS):

        df_analysis_9 = self.df_Units.join(self.df_Damages,self.df_Units["CRASH_ID"]==self.df_Damages["CRASH_ID"],"leftanti")\
                        .filter("((VEH_DMAG_SCL_1_ID not in ('INVALID VALUE','NA','NO DAMAGE') and VEH_DMAG_SCL_1_ID>'DAMAGED 4') or (VEH_DMAG_SCL_2_ID not in ('INVALID VALUE','NA','NO DAMAGE') and VEH_DMAG_SCL_2_ID>'DAMAGED 4')) and fin_resp_type_id in ('PROOF OF LIABILITY INSURANCE','LIABILITY INSURANCE POLICY')")\
                        .select("unt.crash_id")\
                        .distinct()\
                        .agg(count("*").alias("count"))\
                        .select("count")
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_9.write.format("csv").save(output_config[V_PROCESS])
    
    def Analysis_10(self,log_process,batch_id,V_PARENT_PROCESS,V_STAGE,V_PROCESS):

        df_top_25_states=self.df_Units.groupBy("veh_lic_state_id").agg(count("*").alias("total_count")).orderBy(desc("total_count")).drop("total_count").limit(25)
        df_top_10_colors=self.df_Units.groupBy("veh_color_id").agg(count("*").alias("total_count")).orderBy(desc("total_count")).drop("total_count").limit(10)

        df_analysis_10 = self.df_Units.join(self.df_Primary_Person,(self.df_Primary_Person["CRASH_ID"]==self.df_Units["CRASH_ID"]) & (self.df_Primary_Person["UNIT_NBR"]==self.df_Units["UNIT_NBR"]))\
                         .join(self.df_Charges,(self.df_Primary_Person["CRASH_ID"]==self.df_Charges["CRASH_ID"]) & (self.df_Primary_Person["UNIT_NBR"]==self.df_Charges["UNIT_NBR"]) & (self.df_Primary_Person["PRSN_NBR"]==self.df_Charges["PRSN_NBR"]))\
                         .join(df_top_25_states,df_top_25_states["veh_lic_state_id"]==self.df_Units["veh_lic_state_id"])\
                         .join(df_top_10_colors,df_top_10_colors["veh_color_id"]==self.df_Units["veh_color_id"])\
                         .filter("DRVR_LIC_TYPE_ID IN ('COMMERCIAL DRIVER LIC.','DRIVER LICENSE') and prsn_type_id = 'DRIVER' AND upper(CHARGE) like '%SPEED%'")\
                         .groupBy("veh_make_id")\
                         .agg(count("*").alias("count"))\
                         .orderBy(desc("count"))\
                         .limit(5)
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_10.write.format("csv").save(output_config[V_PROCESS])
    

# COMMAND ----------

dbutils.notebook.exit("Completed configuring Analysis")
