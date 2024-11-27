# Databricks notebook source
# MAGIC %md
# MAGIC This command runs the specified Python file, which contains the definition of the UDF_CSV class used in the script.
# MAGIC
# MAGIC Usage:
# MAGIC     %run /Users/gokulleburu@gmail.com/BCG_Case_Study_Process/UDF/UDF_CSV
# MAGIC
# MAGIC Details:
# MAGIC     - The file located at /Users/gokulleburu@gmail.com/BCG_Case_Study_Process/UDF/UDF_CSV should define the UDF_CSV class.
# MAGIC     - This class is used to read CSV files based on the configuration provided in the script.
# MAGIC     - Ensure that the file path is correct and accessible before running this command.
# MAGIC
# MAGIC Note:
# MAGIC     This command should be executed before running the main script to ensure that the UDF_CSV class is available for use.

# COMMAND ----------

# MAGIC %run /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Utility/UDF_CSV

# COMMAND ----------

# MAGIC %md
# MAGIC This command runs the specified Python file, which contains the definition of the Log_process class used for logging the status of various processes.
# MAGIC
# MAGIC Usage:
# MAGIC     %run /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Utility/Log_process
# MAGIC
# MAGIC Details:
# MAGIC     - The file located at /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Utility/Log_process should define the Log_process class.
# MAGIC     - This class is used to log the status of different stages and processes, manage batch IDs, and handle errors.
# MAGIC     - Ensure that the file path is correct and accessible before running this command.
# MAGIC
# MAGIC Note:
# MAGIC     This command should be executed before running the main script to ensure that the Log_process class is available for use.
# MAGIC """

# COMMAND ----------

# MAGIC %run /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Utility/Log_process

# COMMAND ----------

"""
This script reads configuration data from a JSON file and uses it to load multiple CSV files into Spark DataFrames. 
It also removes duplicates from some of the DataFrames and assigns aliases for easier reference.

Dependencies:
    - pyspark.sql
    - pyspark.sql.types
    - pyspark.sql.functions
    - pyspark.sql.window

Usage:
    %run /path/to/your/script

Steps:
    1. Import necessary modules from PySpark.
    2. Initialize V_STAGE, V_TOTAL_STAGES, V_PROCESS, and V_PARENT_PROCESS variables.
    3. Read the JSON configuration file from the specified path.
    4. Initialize the Log_process class with the total number of stages.
    5. Get a new batch ID using the Log_process class.
    6. Extract 'Input' and 'Output' configurations as dictionaries from the JSON configuration DataFrame.
    7. Initialize the UDF_CSV class with the input configuration.
    8. Load the 'Units' CSV file into a DataFrame, remove duplicates, and assign an alias.
    9. Load the 'Primary Person' CSV file into a DataFrame and assign an alias.
    10. Load the 'Charges' CSV file into a DataFrame, remove duplicates, and assign an alias.
    11. Load the 'Damages' CSV file into a DataFrame, remove duplicates, and assign an alias.
    12. Load the 'Endorse' CSV file into a DataFrame and assign an alias.
    13. Load the 'Restrict' CSV file into a DataFrame and assign an alias.

Note:
    Ensure that the JSON configuration file path is correct and accessible.
    The Log_process and UDF_CSV classes should be defined and available in the specified path before running this script.
"""

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

V_STAGE = 0
V_TOTAL_STAGES = 10
V_PROCESS =""
V_PARENT_PROCESS = "vehicle_crash"

# Read the JSON configuration file
config_df = spark.read.option("multiline", "true").json("dbfs:/FileStore/Config.json")

#Initialize log_process class object
log_process = Log_process(V_TOTAL_STAGES)
# Get a new batch ID
batch_id = log_process.get_new_batch_id(V_PARENT_PROCESS)
print("starting the batch:",batch_id)

# Extract the 'Input' and 'Output' columns as dictionaries
input_config = config_df.select("Input").rdd.flatMap(lambda x: x).collect()[0].asDict()
output_config = config_df.select("Output").rdd.flatMap(lambda x: x).collect()[0].asDict()

#Initialize UDF_CSV class object
udf_csv = UDF_CSV(input_config)

df_Units = udf_csv.get_csv("Units")
#found duplicates in df_units, so removing them
df_Units= df_Units.dropDuplicates().alias("unt")

df_Primary_Person = udf_csv.get_csv("Primary Person").alias("pp")

df_Charges = udf_csv.get_csv("Charges")
#found duplicates in df_Charges, so removing them
df_Charges = df_Charges.dropDuplicates().alias("chr")

df_Damages = udf_csv.get_csv("Damages")
#found duplicates in df_Damages, so removing them
df_Damages = df_Damages.dropDuplicates().alias("dmg")

df_Endorse = udf_csv.get_csv("Endorse").alias("end")

df_Restrict = udf_csv.get_csv("Restrict").alias("rstr")


# COMMAND ----------

# DBTITLE 1,Analysis 1
"""
This script performs an analysis to count the number of crashes where more than two males were killed. 
It logs the status of the process and handles errors appropriately.

Steps:
    1. Increment the stage counter.
    2. Define the process name as "Analysis_1".
    3. Check if the process has already been completed for the given batch ID.
    4. If not completed, log the process as started.
    5. Filter the 'Primary Person' DataFrame to include only records where the person is male was killed.
    6. Group by 'CRASH_ID' and count the number of males killed in each crash.
    7. Filter to include only crashes where more than two males were killed.
    8. Count the total number of such crashes and select the result.
    9. Remove any existing output file for the process.
    10. Save the result as a CSV file to the specified output path.
    11. Log the process as completed.
    12. If the process was already completed, skip to the next process.
    13. Display the result of the analysis.
    14. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The name of the current process.
    V_PARENT_PROCESS (str): The name of the parent process.
    log_process (Log_process): An instance of the Log_process class for logging process status.
    batch_id (int): The batch ID for the current process.
    df_Primary_Person (DataFrame): The DataFrame containing primary person data.
    output_config (dict): A dictionary containing the output configuration paths.

Note:
    Ensure that the Log_process class is defined and available before running this script.
    The 'Primary Person' DataFrame should be loaded and available as df_Primary_Person.
    The output configuration dictionary should be defined and contain the path for the current process.
"""
#we can take PRSN_INJRY_SEV_ID or death_cnt , here death cnt is only 1 or 0 so i went with  prsn_injry_sev_id

V_STAGE = V_STAGE+1
V_PROCESS="Analysis_1"
try:
    if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='" +V_PROCESS+"' and status='completed'").count()) == 0):
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")
        df_analysis_1 = df_Primary_Person.filter("pp.PRSN_GNDR_ID == 'MALE' and pp.PRSN_INJRY_SEV_ID='KILLED'")\
                               .groupBy("CRASH_ID")\
                               .agg(count("*").alias("No_of_male_killed"))\
                               .filter("No_of_male_killed>2")\
                               .agg(count("*").alias("Total_No_of_crash_id_analysis_1"))\
                               .select("Total_No_of_crash_id_analysis_1")
        #display(df_analysis_1)
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_1.write.format("csv").save(output_config[V_PROCESS])

        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
    else:
        print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
    display(spark.read.format("csv").load(output_config[V_PROCESS]))
except Exception as e:
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise

# COMMAND ----------

# DBTITLE 1,Analysis 2
"""
This script performs an analysis to count the total number of crashes involving two wheelers. 
It logs the status of the process and handles errors appropriately.

Steps:
    1. Increment the stage counter.
    2. Define the process name as "Analysis_2".
    3. Check if the process has already been completed for the given batch ID.
    4. If not completed, log the process as started.
    5. Filter the 'Units' DataFrame to include only records where the vehicle body style is 'MOTORCYCLE'.
    6. Count the total number of such crashes and select the result.
    7. Remove any existing output file for the process.
    8. Save the result as a CSV file to the specified output path.
    9. Log the process as completed.
    10. If the process was already completed, skip to the next process.
    11. Display the result of the analysis.
    12. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The name of the current process.
    V_PARENT_PROCESS (str): The name of the parent process.
    log_process (Log_process): An instance of the Log_process class for logging process status.
    batch_id (int): The batch ID for the current process.
    df_Units (DataFrame): The DataFrame containing units data.
    output_config (dict): A dictionary containing the output configuration paths.

Note:
    Ensure that the Log_process class is defined and available before running this script.
    The 'Units' DataFrame should be loaded and available as df_Units.
    The output configuration dictionary should be defined and contain the path for the current process.
"""
V_STAGE = V_STAGE+1
V_PROCESS="Analysis_2"
try:
    if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='" +V_PROCESS+"' and status='completed'").count()) == 0):
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")
        df_analysis_2 = df_Units.filter("veh_body_styl_id ='MOTORCYCLE'")\
                               .agg(count("*").alias("Total_No_of_two_wheeler_crash_analysis_1"))\
                               .select("Total_No_of_two_wheeler_crash_analysis_1")
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_2.write.format("csv").save(output_config[V_PROCESS])

        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
    else:
        print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
    display(spark.read.format("csv").load(output_config[V_PROCESS]))
except Exception as e:
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise


# COMMAND ----------

# DBTITLE 1,Analysis 3
"""
This script performs an analysis to identify the top 5 vehicle makes involved in crashes where the driver was killed and the airbag did not deploy.
It logs the status of the process and handles errors appropriately.

Steps:
    1. Increment the stage counter.
    2. Define the process name as "Analysis_3".
    3. Check if the process has already been completed for the given batch ID.
    4. If not completed, log the process as started.
    5. Join the 'Units' and 'Primary Person' DataFrames on 'CRASH_ID' and 'UNIT_NBR'.
    6. Filter the joined DataFrame to include only records where the person was killed, the airbag did not deploy, and the vehicle make is known.
    7. Group by 'VEH_MAKE_ID' and count the number of vehicles per brand.
    8. Order the result by the count in descending order and limit to the top 5 vehicle brands.
    9. Remove any existing output file for the process.
    10. Save the result as a CSV file to the specified output path.
    11. Log the process as completed.
    12. If the process was already completed, skip to the next process.
    13. Display the result of the analysis.
    14. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The name of the current process.
    V_PARENT_PROCESS (str): The name of the parent process.
    log_process (Log_process): An instance of the Log_process class for logging process status.
    batch_id (int): The batch ID for the current process.
    df_Units (DataFrame): The DataFrame containing units data.
    df_Primary_Person (DataFrame): The DataFrame containing primary person data.
    output_config (dict): A dictionary containing the output configuration paths.

Note:
    Ensure that the Log_process class is defined and available before running this script.
    The 'Units' and 'Primary Person' DataFrames should be loaded and available as df_Units and df_Primary_Person, respectively.
    The output configuration dictionary should be defined and contain the path for the current process.
"""
V_STAGE = V_STAGE+1
V_PROCESS="Analysis_3"
try:
    if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='" +V_PROCESS+"' and status='completed'").count()) == 0):
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")
        df_analysis_3 = df_Units.join(df_Primary_Person,(df_Primary_Person["CRASH_ID"]==df_Units["CRASH_ID"]) & (df_Primary_Person["UNIT_NBR"]==df_Units["UNIT_NBR"]))\
                        .filter("PRSN_INJRY_SEV_ID = 'KILLED' AND PRSN_AIRBAG_ID = 'NOT DEPLOYED' AND VEH_MAKE_ID!='NA'")\
                        .groupBy("VEH_MAKE_ID")\
                        .agg(count("*").alias("No_of_veh_per_brand"))\
                        .orderBy(desc("No_of_veh_per_brand"))\
                        .limit(5)
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_3.write.format("csv").save(output_config[V_PROCESS])

        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
    else:
        print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
    display(spark.read.format("csv").load(output_config[V_PROCESS]))
except Exception as e:
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise

# COMMAND ----------

# DBTITLE 1,Analysis 4
"""
This script performs an analysis to count the number of crashes involving vehicles involued in HIT and RUN and drivers with specific license types. 
It logs the status of the process and handles errors appropriately.

Steps:
    1. Increment the stage counter.
    2. Define the process name as "Analysis_4".
    3. Check if the process has already been completed for the given batch ID.
    4. If not completed, log the process as started.
    5. Join the 'Units' and 'Primary Person' DataFrames on 'CRASH_ID' and 'UNIT_NBR'.
    6. Filter the joined DataFrame to include only records where the vehicle involued in HIT and RUN and the driver has a commercial or regular driver's license.
    7. Count the total number of such crashes.
    8. Remove any existing output file for the process.
    9. Save the result as a CSV file to the specified output path.
    10. Log the process as completed.
    11. If the process was already completed, skip to the next process.
    12. Display the result of the analysis.
    13. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The name of the current process.
    V_PARENT_PROCESS (str): The name of the parent process.
    log_process (Log_process): An instance of the Log_process class for logging process status.
    batch_id (int): The batch ID for the current process.
    df_Units (DataFrame): The DataFrame containing units data.
    df_Primary_Person (DataFrame): The DataFrame containing primary person data.
    output_config (dict): A dictionary containing the output configuration paths.

Note:
    Ensure that the Log_process class is defined and available before running this script.
    The 'Units' and 'Primary Person' DataFrames should be loaded and available as df_Units and df_Primary_Person, respectively.
    The output configuration dictionary should be defined and contain the path for the current process.
"""

V_STAGE = V_STAGE+1
V_PROCESS="Analysis_4"
try:
    if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='" +V_PROCESS+"' and status='completed'").count()) == 0):
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")
        df_analysis_4 = df_Units.join(df_Primary_Person,(df_Primary_Person["CRASH_ID"]==df_Units["CRASH_ID"]) & (df_Primary_Person["UNIT_NBR"]==df_Units["UNIT_NBR"]))\
                               .filter("VEH_HNR_FL='Y' AND DRVR_LIC_TYPE_ID IN ('COMMERCIAL DRIVER LIC.' , 'DRIVER LICENSE')")\
                               .agg(count("*").alias("count"))
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_4.write.format("csv").save(output_config[V_PROCESS])

        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
    else:
        print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
    display(spark.read.format("csv").load(output_config[V_PROCESS]))
except Exception as e:
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise

# COMMAND ----------

# DBTITLE 1,Analysis 5
"""
This script performs an analysis to identify the state with the highest number of accidents not involving female drivers with known license states. 
It logs the status of the process and handles errors appropriately.

Steps:
    1. Increment the stage counter.
    2. Define the process name as "Analysis_5".
    3. Check if the process has already been completed for the given batch ID.
    4. If not completed, log the process as started.
    5. Filter the 'Primary Person' DataFrame to include only records where the person is not female and the driver's license state is known.
    6. Group by 'DRVR_LIC_STATE_ID' and count the total number of accidents for each state.
    7. Order the result by the count in descending order and limit to the state with the highest number of accidents.
    8. Remove any existing output file for the process.
    9. Save the result as a CSV file to the specified output path.
    10. Log the process as completed.
    11. If the process was already completed, skip to the next process.
    12. Display the result of the analysis.
    13. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The name of the current process.
    V_PARENT_PROCESS (str): The name of the parent process.
    log_process (Log_process): An instance of the Log_process class for logging process status.
    batch_id (int): The batch ID for the current process.
    df_Primary_Person (DataFrame): The DataFrame containing primary person data.
    output_config (dict): A dictionary containing the output configuration paths.

Note:
    Ensure that the Log_process class is defined and available before running this script.
    The 'Primary Person' DataFrame should be loaded and available as df_Primary_Person.
    The output configuration dictionary should be defined and contain the path for the current process.
"""
V_STAGE = V_STAGE+1
V_PROCESS="Analysis_5"
try:
    if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='" +V_PROCESS+"' and status='completed'").count()) == 0):
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")
        df_analysis_5 = df_Primary_Person.filter("PRSN_GNDR_ID !='FEMALE' and DRVR_LIC_STATE_ID not in ('NA','Unknown')")\
                                 .groupBy("DRVR_LIC_STATE_ID")\
                                 .agg(count("*").alias("Total_no_of_accidents"))\
                                 .orderBy(desc("Total_no_of_accidents"))\
                                 .limit(1)
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_5.write.format("csv").save(output_config[V_PROCESS])
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
    else:
        print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
    display(spark.read.format("csv").load(output_config[V_PROCESS]))
except Exception as e:
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise

# COMMAND ----------

# DBTITLE 1,Analysis 6
"""
This script performs an analysis to identify the vehicle makes ranked 3rd to 5th in terms of the total number of injuries including deaths. 
It logs the status of the process and handles errors appropriately.

Steps:
    1. Increment the stage counter.
    2. Define the process name as "Analysis_6".
    3. Check if the process has already been completed for the given batch ID.
    4. If not completed, log the process as started.
    5. Define a window specification to order by the total number of injuries and deaths in descending order.
    6. Filter the 'Units' DataFrame to exclude records with unknown or NA vehicle makes.
    7. Group by 'veh_make_id' and calculate the total number of injuries and deaths.
    8. Add a row number column based on the window specification.
    9. Filter to include only the vehicle makes ranked 3rd to 5th.
    10. Select the 'veh_make_id' column.
    11. Remove any existing output file for the process.
    12. Save the result as a CSV file to the specified output path.
    13. Log the process as completed.
    14. If the process was already completed, skip to the next process.
    15. Display the result of the analysis.
    16. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The name of the current process.
    V_PARENT_PROCESS (str): The name of the parent process.
    log_process (Log_process): An instance of the Log_process class for logging process status.
    batch_id (int): The batch ID for the current process.
    df_Units (DataFrame): The DataFrame containing units data.
    output_config (dict): A dictionary containing the output configuration paths.

Note:
    Ensure that the Log_process class is defined and available before running this script.
    The 'Units' DataFrame should be loaded and available as df_Units.
    The output configuration dictionary should be defined and contain the path for the current process.
"""
V_STAGE = V_STAGE+1
V_PROCESS="Analysis_6"
try:
    if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='" +V_PROCESS+"' and status='completed'").count()) == 0):
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")
        window_spec = Window.orderBy(desc("Total_no_of_injurys_and_deaths"))
        df_analysis_6= df_Units.filter("veh_make_id not in ('NA','UNKNOWN')")\
                       .groupBy("veh_make_id")\
                       .agg((sum("DEATH_CNT")+sum("TOT_INJRY_CNT")).alias("Total_no_of_injurys_and_deaths"))\
                       .withColumn("rk",row_number().over(window_spec))\
                       .filter("rk between 3 and 5")\
                       .select("veh_make_id")
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_6.write.format("csv").save(output_config[V_PROCESS])

        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
    else:
        print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
    display(spark.read.format("csv").load(output_config[V_PROCESS]))
except Exception as e:
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise

# COMMAND ----------

# DBTITLE 1,Analysis 7
"""
This script performs an analysis to identify the most common ethnicity for each vehicle body style involved in crashes. 
It logs the status of the process and handles errors appropriately.

Steps:
    1. Increment the stage counter.
    2. Define the process name as "Analysis_7".
    3. Check if the process has already been completed for the given batch ID.
    4. If not completed, log the process as started.
    5. Define a window specification to partition by vehicle body style and order by the total count in descending order.
    6. Join the 'Units' and 'Primary Person' DataFrames on 'CRASH_ID' and 'UNIT_NBR'.
    7. Filter the joined DataFrame to exclude records with unknown or NA vehicle body styles and ethnicities.
    8. Group by 'veh_body_styl_id' and 'PRSN_ETHNICITY_ID' and count the total number of records for each group.
    9. Add a row number column based on the window specification.
    10. Order the result by vehicle body style, total count in descending order, and ethnicity.
    11. Filter to include only the top record for each vehicle body style.
    12. Select the 'veh_body_styl_id', 'PRSN_ETHNICITY_ID', and 'Total_count' columns.
    13. Remove any existing output file for the process.
    14. Save the result as a CSV file to the specified output path.
    15. Log the process as completed.
    16. If the process was already completed, skip to the next process.
    17. Display the result of the analysis.
    18. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The name of the current process.
    V_PARENT_PROCESS (str): The name of the parent process.
    log_process (Log_process): An instance of the Log_process class for logging process status.
    batch_id (int): The batch ID for the current process.
    df_Units (DataFrame): The DataFrame containing units data.
    df_Primary_Person (DataFrame): The DataFrame containing primary person data.
    output_config (dict): A dictionary containing the output configuration paths.

Note:
    Ensure that the Log_process class is defined and available before running this script.
    The 'Units' and 'Primary Person' DataFrames should be loaded and available as df_Units and df_Primary_Person, respectively.
    The output configuration dictionary should be defined and contain the path for the current process.
"""
V_STAGE = V_STAGE+1
V_PROCESS="Analysis_7"
try:
    if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='" +V_PROCESS+"' and status='completed'").count()) == 0):
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")

        window_spec = Window.partitionBy("veh_body_styl_id").orderBy(desc("Total_count"))

        df_analysis_7 = df_Units.join(df_Primary_Person,(df_Primary_Person["CRASH_ID"]==df_Units["CRASH_ID"]) & (df_Primary_Person["UNIT_NBR"]==df_Units["UNIT_NBR"]))\
                        .filter("veh_body_styl_id not in ('NA','UNKNOWN','OTHER  (EXPLAIN IN NARRATIVE)','NOT REPORTED') and PRSN_ETHNICITY_ID not in ('NA','UNKNOWN')")\
                        .groupBy("veh_body_styl_id","PRSN_ETHNICITY_ID")\
                        .agg(count("*").alias("Total_count"))\
                        .withColumn("rn",row_number().over(window_spec))\
                        .orderBy("veh_body_styl_id",desc("Total_count"),"PRSN_ETHNICITY_ID")\
                        .filter("rn=1")\
                        .select("veh_body_styl_id","PRSN_ETHNICITY_ID","Total_count")
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_7.write.format("csv").save(output_config[V_PROCESS])

        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
    else:
        print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
    display(spark.read.format("csv").load(output_config[V_PROCESS]))
except Exception as e:
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise

# COMMAND ----------

# DBTITLE 1,Analysis 8
"""
This script performs an analysis to identify the top 5 driver ZIP codes associated with crashes where the driver was under the influence of alcohol. 
It logs the status of the process and handles errors appropriately.

Steps:
    1. Increment the stage counter.
    2. Define the process name as "Analysis_8".
    3. Check if the process has already been completed for the given batch ID.
    4. If not completed, log the process as started.
    5. Join the 'Units' and 'Primary Person' DataFrames on 'CRASH_ID' and 'UNIT_NBR'.
    6. Filter the joined DataFrame to include only records where the contributing factor was alcohol-related and the driver's ZIP code is not null.
    7. Group by 'DRVR_ZIP' and count the total number of records for each ZIP code.
    8. Order the result by the total count in descending order and limit to the top 5 ZIP codes.
    9. Remove any existing output file for the process.
    10. Save the result as a CSV file to the specified output path.
    11. Log the process as completed.
    12. If the process was already completed, skip to the next process.
    13. Display the result of the analysis.
    14. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The name of the current process.
    V_PARENT_PROCESS (str): The name of the parent process.
    log_process (Log_process): An instance of the Log_process class for logging process status.
    batch_id (int): The batch ID for the current process.
    df_Units (DataFrame): The DataFrame containing units data.
    df_Primary_Person (DataFrame): The DataFrame containing primary person data.
    output_config (dict): A dictionary containing the output configuration paths.

Note:
    Ensure that the Log_process class is defined and available before running this script.
    The 'Units' and 'Primary Person' DataFrames should be loaded and available as df_Units and df_Primary_Person, respectively.
    The output configuration dictionary should be defined and contain the path for the current process.
"""
V_STAGE = V_STAGE+1
V_PROCESS="Analysis_8"
try:
    if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='" +V_PROCESS+"' and status='completed'").count()) == 0):
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")

        df_analysis_8 = df_Units.join(df_Primary_Person,(df_Primary_Person["CRASH_ID"]==df_Units["CRASH_ID"]) & (df_Primary_Person["UNIT_NBR"]==df_Units["UNIT_NBR"]))\
                        .filter("(CONTRIB_FACTR_1_ID in ('HAD BEEN DRINKING','UNDER INFLUENCE - ALCOHOL') or CONTRIB_FACTR_2_ID in ('UNDER INFLUENCE - ALCOHOL','HAD BEEN DRINKING') or CONTRIB_FACTR_P1_ID in ('HAD BEEN DRINKING','UNDER INFLUENCE - ALCOHOL')) and DRVR_ZIP is not null")\
                        .groupBy("DRVR_ZIP")\
                        .agg(count("*").alias("Total_count"))\
                        .orderBy(desc("Total_count"))\
                        .limit(5)
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_8.write.format("csv").save(output_config[V_PROCESS])

        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
    else:
        print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
    display(spark.read.format("csv").load(output_config[V_PROCESS]))
except Exception as e:
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise

# COMMAND ----------

# DBTITLE 1,Analysis 9
"""
This script performs an analysis to count the number of crashes involving vehicles with no significant damage, and proof of liability insurance. 
It logs the status of the process and handles errors appropriately.

Steps:
    1. Increment the stage counter.
    2. Define the process name as "Analysis_9".
    3. Check if the process has already been completed for the given batch ID.
    4. If not completed, log the process as started.
    5. Join the 'Units' and 'Damages' DataFrames on 'CRASH_ID'.
    6. Filter the joined DataFrame to include only records where the damaged property is not null, the vehicle damage scale is greater than 'DAMAGED 4', and the financial responsibility type is proof of liability insurance.
    7. Select distinct crash IDs.
    8. Count the total number of distinct crash IDs.
    9. Remove any existing output file for the process.
    10. Save the result as a CSV file to the specified output path.
    11. Log the process as completed.
    12. If the process was already completed, skip to the next process.
    13. Display the result of the analysis.
    14. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The name of the current process.
    V_PARENT_PROCESS (str): The name of the parent process.
    log_process (Log_process): An instance of the Log_process class for logging process status.
    batch_id (int): The batch ID for the current process.
    df_Units (DataFrame): The DataFrame containing units data.
    df_Damages (DataFrame): The DataFrame containing damages data.
    output_config (dict): A dictionary containing the output configuration paths.

Note:
    Ensure that the Log_process class is defined and available before running this script.
    The 'Units' and 'Damages' DataFrames should be loaded and available as df_Units and df_Damages, respectively.
    The output configuration dictionary should be defined and contain the path for the current process.
"""
V_STAGE = V_STAGE+1
V_PROCESS="Analysis_9"
try:
    if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='" +V_PROCESS+"' and status='completed'").count()) == 0):
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")

        df_analysis_9 = df_Units.join(df_Damages,df_Units["CRASH_ID"]==df_Damages["CRASH_ID"],"leftanti")\
                        .filter("((VEH_DMAG_SCL_1_ID not in ('INVALID VALUE','NA','NO DAMAGE') and VEH_DMAG_SCL_1_ID>'DAMAGED 4') or (VEH_DMAG_SCL_2_ID not in ('INVALID VALUE','NA','NO DAMAGE') and VEH_DMAG_SCL_2_ID>'DAMAGED 4')) and fin_resp_type_id in ('PROOF OF LIABILITY INSURANCE','LIABILITY INSURANCE POLICY')")\
                        .select("unt.crash_id")\
                        .distinct()\
                        .agg(count("*").alias("count"))\
                        .select("count")
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_9.write.format("csv").save(output_config[V_PROCESS])

        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
    else:
        print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
    display(spark.read.format("csv").load(output_config[V_PROCESS]))
except Exception as e:
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise

# COMMAND ----------

# DBTITLE 1,Analysis 10
"""
This script performs an analysis to identify the top 5 vehicle makes involved in speeding-related crashes, where the driver has a commercial or regular driver's license, and the vehicle is among the top 25 states and top 10 colors. 
It logs the status of the process and handles errors appropriately.

Steps:
    1. Increment the stage counter.
    2. Define the process name as "Analysis_10".
    3. Check if the process has already been completed for the given batch ID.
    4. If not completed, log the process as started.
    5. Group the 'Units' DataFrame by vehicle license state and count the total number of records for each state.
    6. Order the result by the total count in descending order and limit to the top 25 states.
    7. Group the 'Units' DataFrame by vehicle color and count the total number of records for each color.
    8. Order the result by the total count in descending order and limit to the top 10 colors.
    9. Join the 'Units', 'Primary Person', and 'Charges' DataFrames on 'CRASH_ID', 'UNIT_NBR', and 'PRSN_NBR'.
    10. Join the result with the top 25 states and top 10 colors DataFrames.
    11. Filter the joined DataFrame to include only records where the driver has a commercial or regular driver's license, the person type is 'DRIVER', and the charge includes 'SPEED'.
    12. Group by 'veh_make_id' and count the total number of records for each vehicle make.
    13. Order the result by the count in descending order and limit to the top 5 vehicle makes.
    14. Remove any existing output file for the process.
    15. Save the result as a CSV file to the specified output path.
    16. Log the process as completed.
    17. If the process was already completed, skip to the next process.
    18. Display the result of the analysis.
    19. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The name of the current process.
    V_PARENT_PROCESS (str): The name of the parent process.
    log_process (Log_process): An instance of the Log_process class for logging process status.
    batch_id (int): The batch ID for the current process.
    df_Units (DataFrame): The DataFrame containing units data.
    df_Primary_Person (DataFrame): The DataFrame containing primary person data.
    df_Charges (DataFrame): The DataFrame containing charges data.
    output_config (dict): A dictionary containing the output configuration paths.

Note:
    Ensure that the Log_process class is defined and available before running this script.
    The 'Units', 'Primary Person', and 'Charges' DataFrames should be loaded and available as df_Units, df_Primary_Person, and df_Charges, respectively.
    The output configuration dictionary should be defined and contain the path for the current process.
"""

V_STAGE = V_STAGE+1
V_PROCESS="Analysis_10"
try:
    if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='" +V_PROCESS+"' and status='completed'").count()) == 0):
        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")

        df_top_25_states=df_Units.groupBy("veh_lic_state_id").agg(count("*").alias("total_count")).orderBy(desc("total_count")).drop("total_count").limit(25)
        df_top_10_colors=df_Units.groupBy("veh_color_id").agg(count("*").alias("total_count")).orderBy(desc("total_count")).drop("total_count").limit(10)

        df_analysis_10 = df_Units.join(df_Primary_Person,(df_Primary_Person["CRASH_ID"]==df_Units["CRASH_ID"]) & (df_Primary_Person["UNIT_NBR"]==df_Units["UNIT_NBR"]))\
                         .join(df_Charges,(df_Primary_Person["CRASH_ID"]==df_Charges["CRASH_ID"]) & (df_Primary_Person["UNIT_NBR"]==df_Charges["UNIT_NBR"]) & (df_Primary_Person["PRSN_NBR"]==df_Charges["PRSN_NBR"]))\
                         .join(df_top_25_states,df_top_25_states["veh_lic_state_id"]==df_Units["veh_lic_state_id"])\
                         .join(df_top_10_colors,df_top_10_colors["veh_color_id"]==df_Units["veh_color_id"])\
                         .filter("DRVR_LIC_TYPE_ID IN ('COMMERCIAL DRIVER LIC.','DRIVER LICENSE') and prsn_type_id = 'DRIVER' AND upper(CHARGE) like '%SPEED%'")\
                         .groupBy("veh_make_id")\
                         .agg(count("*").alias("count"))\
                         .orderBy(desc("count"))\
                         .limit(5)
        dbutils.fs.rm(output_config[V_PROCESS],True)
        df_analysis_10.write.format("csv").save(output_config[V_PROCESS])

        log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
    else:
        print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
    display(spark.read.format("csv").load(output_config[V_PROCESS]))
except Exception as e:
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise

# COMMAND ----------

dbutils.notebook.exit("Completed the analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from log_table

# COMMAND ----------

display(df_Units)

# COMMAND ----------

display(df_Primary_Person)




# COMMAND ----------

display(df_Charges)

# COMMAND ----------

display(df_Damages)

# COMMAND ----------

display(df_Endorse)

# DRVR_LIC_ENDORS_ID
# SCHOOL BUS
# OTHER/OUT OF STATE
# UNKNOWN
# HAZARDOUS MATERIALS
# UNLICENSED
# TANK VEHICLE
# NONE
# TANK VEHICLE WITH HAZARDOUS MATERIALS
# PASSENGER
# DOUBLE/TRIPLE TRAILER

# COMMAND ----------

display(df_Restrict)

# DRVR_LIC_RESTRIC_ID
# MOPED (EFF. END 12/31/2016)
# APPLICABLE PROSTHETIC DEVICES
# FRSI CDL MM/DD/YY - MM/DD/YY OR EXEMPT A VEH
# TRC 545.424 APPLIES UNTIL MM/DD/YY
# TO/FROM WORK OR LOFS 21 OR OVER
# INTRASTATE ONLY (EFF. 1/1/2017)
# WITH CORRECTIVE LENSES
# STATED ON LICENSE
# OPERATION CLASS A EXEMPT VEH AUTHORIZED
# VEHICLE NOT TO EXCEED CLASS C (EFF. END 12/31/2016)
# NO CLASS A AND B PASSENGER VEHICLE (EFF. 1/1/2017)
# IF CMV, CUSTOM-HARVESTING INTERSTATE
# LOFS 21 OR OVER SCHOOL BUS ONLY
# PASSENGER CMVS RESTRICT TO CLASS C ONLY
# IF CMV, PRIVATELY TRANS PASSENGERS INTERSTATE
# OPERATION CLASS B EXEMPT VEH AUTHORIZED
# LOFS 21 OR OVER VEHICLE ABOVE CLASS C
# IGNITION INTERLOCK REQUIRED (EFF. END 12/31/2016)
# LOFS 21 OR OVER IN VEH EQUIP W/ AIRBRAKE
# BUS NOT TO EXCEED 26,000 LBS GVWR
# IF CMV, GOVERNMENT VEHICLES INTERSTATE
# MEDICAL VARIANCE
# OTHER/OUT OF STATE
# IF CMV, USE IN OIL/WATER WELL SERVICE/DRILL
# HME EXPIRATION DATE MM/DD/YY
# FRSI CDL MM/DD/YY - MM/DD/YY OR EXEMPT B VEH
# OCC./ESSENT. NEED DL-NO CMV-SEE COURT ORDER (EFF. END 12/31/2016)
# MUST HOLD VALID LEARNER LIC. TO MM/DD/YY
# SPEED NOT TO EXCEED 45 MPH
# LICENSED MC OPERATOR 21 OR OVER IN SIGHT
# IF CMV, FOR OPERATION OF MOBILE CRANE
# UNKNOWN
# TO/FROM WORK/SCHOOL
# TO/FROM WORK/SCHOOL OR LOFS 21 OR OVER
# IF CMV, TRANSPORTING BEES/HIVES INTERSTATE
# NO EXPRESSWAY DRIVING (EFF. END 12/31/2016)
# VEHICLE NOT TO EXCEED 26,000 LBS GVWR
# LOFS 21 OR OVER BUS ONLY
# NO AIR BRAKE EQUIPPED CMV
# WITH TELESCOPIC LENS
# IF CMV, TRANS CORPSE/SICK/INJURED INTERSTATE
# LOFS 21 OR OVER
# UNLICENSED
# DAYTIME DRIVING ONLY
# OTHER
# CDL INTRASTATE ONLY (EFF. END 12/31/2016)
# IF CMV, INTRA-CITY ZONE DRIVERS INTERSTATE
# VALID TX VISION OR LIMB WAIVER REQUIRED
# FRSI CDL VALID MM/DD/YY TO MM/DD/YY
# LOFS 21 OR OVER VEHICLE ABOVE CLASS B
# IF CMV, FIRE/RESCUE INTERSTATE
# MC NOT TO EXCEED 250 CC
# NONE
# IF CMV, SCHOOL BUSES INTERSTATE
# FOR CLASS M TRC 545.424 UNTIL MM/DD/YY
# CLASS C ONLY - NO TAXI/BUS/EMERGENCY VEH
# AUTOMATIC TRANSMISSION
# IF CMV, ONLY TRANS PERSONAL PROP INTERSTATE
# OUTSIDE REARVIEWMIRROR OR HEARING AID
# POWER STEERING
# TO/FROM WORK
# APPLICABLE VEHICLE DEVICES (EFF. END 12/31/2016)
