# Databricks notebook source
class config():
    def __init__(self):
        self.base_data_dir = spark.sql("describe external location `data_zone`").select("url").collect()[0][0]
        self.base_dir_checkpt = spark.sql("describe external location `checkpoint`").select("url").collect()[0][0]
        self.db_name = "sbit_db"
        self.maxFilesPerTrigger = 1000
