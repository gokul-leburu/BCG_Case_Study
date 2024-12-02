# Databricks notebook source
# MAGIC %md
# MAGIC This command runs the specified Python file, which contains the definition of the UDF_CSV class used in the script.
# MAGIC
# MAGIC Usage: %run /Users/gokulleburu@gmail.com/BCG_Case_Study_Process/UDF/UDF_CSV
# MAGIC
# MAGIC Details: - The file located at /Users/gokulleburu@gmail.com/BCG_Case_Study_Process/UDF/UDF_CSV should define the UDF_CSV class. - This class is used to read CSV files based on the configuration provided in the script. - Ensure that the file path is correct and accessible before running this command.
# MAGIC
# MAGIC Note: This command should be executed before running the main script to ensure that the UDF_CSV class is available for use.

# COMMAND ----------

# MAGIC %run /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Utility/UDF_CSV

# COMMAND ----------

# MAGIC %md
# MAGIC This command runs the specified Python file, which contains the definition of the Log_process class used for logging the status of various processes.
# MAGIC
# MAGIC Usage: %run /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Utility/Log_process
# MAGIC
# MAGIC Details: - The file located at /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Utility/Log_process should define the Log_process class. - This class is used to log the status of different stages and processes, manage batch IDs, and handle errors. - Ensure that the file path is correct and accessible before running this command.
# MAGIC
# MAGIC Note: This command should be executed before running the main script to ensure that the Log_process class is available for use. """

# COMMAND ----------

# MAGIC %run /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Utility/Log_process

# COMMAND ----------

# MAGIC %md
# MAGIC This command runs the specified Python file, which contains the definition of the Analysis class used for performing various data analyses on vehicle crash data.
# MAGIC
# MAGIC Usage: %run /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Analysis/Analysis
# MAGIC
# MAGIC Details:
# MAGIC - The file located at /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Analysis/Analysis should define the Analysis class.
# MAGIC - This class is used to perform various data analyses on vehicle crash data using Spark DataFrames.
# MAGIC - Ensure that the file path is correct and accessible before running this command.
# MAGIC
# MAGIC Note: This command should be executed before running the main script to ensure that the Analysis class is available for use.

# COMMAND ----------

# MAGIC %run /Users/gokulleburu@gmail.com/Vehicle_Crash_Analytics/Analysis/Analysis

# COMMAND ----------

"""
This cell initializes the necessary components and configurations for performing vehicle crash data analysis using Spark DataFrames.

Steps:
1. Import required libraries and modules from PySpark.
2. Define variables for the stage, total stages, process, and parent process.
3. Read the JSON configuration file containing input and output paths.
4. Initialize the Log_process class object for logging.
5. Retrieve a new batch ID for the current process.
6. Extract input and output configurations from the JSON file.
7. Initialize the UDF_CSV class object for handling CSV files.
8. Initialize the Analysis class object for performing data analyses.

Attributes:
    V_STAGE (int): The current stage of the process.
    V_TOTAL_STAGES (int): The total number of stages in the process.
    V_PROCESS (str): The current process name.
    V_PARENT_PROCESS (str): The parent process name.
    config_df (DataFrame): DataFrame containing the configuration data from the JSON file.
    log_process (Log_process): Instance of the Log_process class for logging.
    batch_id (int): The batch ID for the current process.
    input_config (dict): Dictionary containing input data paths.
    output_config (dict): Dictionary containing output data paths.
    udf_csv (UDF_CSV): Instance of the UDF_CSV class for handling CSV files.
    Analysis (Analysis): Instance of the Analysis class for performing data analyses.

Usage:
    This cell should be executed in the main notebook to set up the environment and initialize the necessary components before running the analyses.
"""

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

V_STAGE = 1
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

#Initialize class Analysis object
Analysis = Analysis(udf_csv,log_process,input_config,output_config)

# COMMAND ----------

"""
This code block iterates through the output configurations, performs the specified analyses, and logs the status of each process.

Steps:
1. Iterate through each process in the output configuration.
2. Check if the process has already been completed for the current batch.
3. If not completed, log the process as started, execute the analysis, and log the process as completed.
4. If already completed, skip to the next process.
5. Display the analysis data for each process.
6. Increment the stage counter after each process.
7. Handle any exceptions by logging the process as failed and re-raising the exception.

Attributes:
    V_PROCESS (str): The current process name.
    output_config (dict): Dictionary containing output data paths.
    log_process (Log_process): Instance of the Log_process class for logging.
    batch_id (int): The batch ID for the current process.
    V_PARENT_PROCESS (str): The parent process name.
    V_STAGE (int): The current stage of the process.
    Analysis (Analysis): Instance of the Analysis class for performing data analyses.

Usage:
    This code block should be executed in the main notebook after initializing the necessary components and configurations. It ensures that each analysis is performed and logged correctly, and handles any errors that may occur during execution.
"""
try:
    for V_PROCESS in output_config:
        if((log_process.log_table.filter("batch_id ="+str(batch_id)+" and parent_process='"+V_PARENT_PROCESS+"' and process='"+V_PROCESS+"' and status='completed'").count()) == 0):
            print("Process:",V_PROCESS,"has started")
            log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "started")
            Analysis.functions[V_PROCESS](V_PROCESS)
            log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "completed")
            print("Process:",V_PROCESS,"has completed")
        else:
            print("Process:",V_PROCESS,"already completed, Skipping and going for next process")
        print("Analysis data for",V_PROCESS,"as below: ")
        spark.read.format("csv").load(output_config[V_PROCESS]).show()
        V_STAGE+=1
except Exception as e:
    print("Process:",V_PROCESS,"has failed")
    # Log the process as failed with the error message
    log_process.log_status(V_STAGE, V_PARENT_PROCESS, V_PROCESS, batch_id, "failed", str(e))
    # Re-raise the exception
    raise 

# COMMAND ----------

dbutils.notebook.exit("Notebook exited: Completed configuring Analysis")
