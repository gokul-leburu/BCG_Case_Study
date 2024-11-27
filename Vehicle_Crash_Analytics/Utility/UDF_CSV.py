# Databricks notebook source
"""
UDF_CSV is a utility class for reading CSV files based on a provided configuration dictionary.

Attributes:
    data_dict (dict): A dictionary containing the mapping of CSV names to their file paths.

Methods:
    __init__(input_config):
        Initializes the UDF_CSV class with the given input configuration dictionary.

    get_csv(csv_name):
        Reads the specified CSV file into a Spark DataFrame.
        Args:
            csv_name (str): The name of the CSV file to read.
        Returns:
            DataFrame: A Spark DataFrame containing the data from the specified CSV file.
        Raises:
            ValueError: If the specified CSV name is not found in the configuration dictionary.

    get_csv_name(csv_name):
        Prints the file paths of the specified CSV files.
        Args:
            csv_name (str): The name of the CSV file to look up. If 'ALL', prints all CSV names and paths.
        Returns:
            None
"""
class UDF_CSV():
    def __init__(self,input_config):
        self.data_dict=input_config
    def get_csv(self,csv_name):
        csv_name= csv_name.upper()
        if csv_name not in self.data_dict:
            raise ValueError(f"Table name '{csv_name}' not found in data_dict")
        print("Table path:",self.data_dict[csv_name])
        CSV_df = spark.read.option("header",True) \
                           .option("inferschema",True) \
                           .csv(self.data_dict[csv_name])
        return CSV_df
    def get_csv_name(self,csv_name):
        csv_name = csv_name.upper()
        if csv_name=="ALL":
            for i in self.data_dict.keys():
                print("Table Name:",i,"And Table Path:",self.data_dict[i])
        else:
            found = False
            for key in self.data_dict:
                if csv_name in key.upper():
                    print("Table Name:", key, "And Table Path:", self.data_dict[key])
                    found = True
            if not found:
                print("Table not found")

# COMMAND ----------

dbutils.notebook.exit("Completed the configuring UDF's")
