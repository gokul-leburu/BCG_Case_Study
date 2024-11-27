# Databricks notebook source
"""
Log_process is a utility class for logging the status of various processes, managing batch IDs, and handling errors in a Spark environment.

Attributes:
    log_table (DataFrame): The DataFrame representing the log table.
    Total_stages (int): The total number of stages in the process.

Methods:
    __init__(Total_stages):
        Initializes the Log_process class.
        Creates the log table if it does not exist.
        Args:
            Total_stages (int): The total number of stages in the process.

    get_new_batch_id(parent_process):
        Retrieves a new batch ID if all processes in the parent process are completed.
        Args:
            parent_process (str): The name of the parent process.
        Returns:
            int: The new batch ID or the existing batch ID if any analysis fails.

    log_status(stage, parent_process, process, batch_id, status, error=None):
        Logs the status of a process.
        Updates the existing record if it exists, otherwise inserts a new record.
        Args:
            stage (str): The current stage of the process.
            parent_process (str): The name of the parent process.
            process (str): The name of the process.
            batch_id (int): The batch ID.
            status (str): The status of the process (e.g., 'started', 'completed', 'failed').
            error (str, optional): The error message if the process failed. Defaults to None.
"""
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Define the updated schema for the log table
log_schema = StructType([
    StructField("stage", StringType(), True),
    StructField("parent_process", StringType(), True),
    StructField("process", StringType(), True),
    StructField("batch_id", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("error", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True)
])

class Log_process:
    def __init__(self,Total_stages):
        if not spark.catalog.tableExists("default.log_table"):
            # Create the log table if it doesn't exist
            # Create an empty DataFrame with the updated schema
            dbutils.fs.rm("dbfs:/user/hive/warehouse/log_table",True)
            empty_log_df = spark.createDataFrame([], log_schema)
            empty_log_df.write.saveAsTable("log_table")
        self.log_table = spark.table("log_table")
        self.Total_stages=Total_stages

    def get_new_batch_id(self,parent_process):
        # Check if the log table has any data
        if self.log_table.count() == 0:
            return 1
        batch_id = self.log_table.filter("parent_process='"+parent_process+"'").agg(max("batch_id")).collect()[0][0]
        # Check if all processes in the parent_description are completed
        completed_processes = self.log_table.filter("parent_process='"+parent_process+"'")\
                                            .filter("status  = 'completed' and batch_id="+str(batch_id)) \
                                            .groupBy("parent_process") \
                                            .agg(count("*").alias("count")) \
                                            .filter("count ="+str(self.Total_stages))  # Assuming 10 child descriptions
        if completed_processes.count() > 0:
            # If all processes are completed, get a new batch ID
            new_batch_id = self.log_table.filter("parent_process='"+parent_process+"'")\
                                         .agg(max("batch_id")).collect()[0][0] + 1
        else:
            # If any analysis fails, pull the existing batch ID
            new_batch_id = batch_id
        return new_batch_id
    
    def log_status(self, stage, parent_process, process, batch_id, status, error=None):
        # Get the current time
        current_time = datetime.now()

        # Load the log table
        log_df = spark.table("log_table")

        # Check if the record exists
        record_exists = log_df.filter(
            (col("parent_process") == parent_process) &
            (col("process") == process) &
            (col("batch_id") == batch_id)
        ).count() > 0

        if record_exists:
            # Update the existing record
            log_df = log_df.withColumn(
                "status",
                when(
                    (col("parent_process") == parent_process) &
                    (col("process") == process) &
                    (col("batch_id") == batch_id),
                    lit(status)
                ).otherwise(col("status"))
            ).withColumn(
                "error",
                when(
                    (col("parent_process") == parent_process) &
                    (col("process") == process) &
                    (col("batch_id") == batch_id),
                    lit(error)
                ).otherwise(col("error"))
            ).withColumn(
                "end_time",
                when(
                    (col("parent_process") == parent_process) &
                    (col("process") == process) &
                    (col("batch_id") == batch_id),
                    lit(current_time)
                ).otherwise(col("end_time"))
            )
        else:
            # Create a new log entry DataFrame
            new_log_entry = [(stage, parent_process, process, batch_id, status, error, current_time, None if status == "started" else current_time)]
            new_log_df = spark.createDataFrame(new_log_entry, schema=log_schema)
            # Append the new log entry to the log table
            log_df = log_df.union(new_log_df)

        # Overwrite the log table with the updated DataFrame
        log_df.write.mode("overwrite").saveAsTable("log_table")

# COMMAND ----------

dbutils.notebook.exit("Completed configuring log_process")
