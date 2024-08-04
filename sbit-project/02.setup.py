# Databricks notebook source
# MAGIC %run "./01.config"

# COMMAND ----------

class setupHelper():
    def __init__(self, env):
        conf = config()
        self.landing_zone = conf.base_data_dir + "/raw"
        self.checkpoint_base = conf.base_dir_checkpt + "/checkpoints"
        self.catalog = "sbit_dev"
        self.db_name = conf.db_name
        self.initialized = False

    def create_db(self):
        spark.catalog.clearCache()
        print(f"Creating database {self.catalog}.{self.db_name} ----->", end="")
        spark.sql(f"create database if not exists {self.catalog}.{self.db_name}")
        spark.sql(f"use {self.catalog}.{self.db_name}")
        self.initialized = True
        print("Database Executed Successfully")

    def create_registered_users_bz(self):
        if self.initialized:
            print(f"Creating registered users table ----->", end="")
            spark.sql(
                f""" create table if not exists {self.catalog}.{self.db_name}.registered_users_bz(
                                user_id long,
                                device_id long,
                                mac_address string,
                                registration_timestamp double,
                                load_time timestamp,
                                source_file string)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_gym_login_bz(self):
        if self.initialized:
            print(f"Creating Gym Login table ----->", end="")
            spark.sql(
                f""" create or replace table {self.catalog}.{self.db_name}.gym_login_bz(
                                mac_address string,
                                gym bigint,
                                login double,
                                logout double,
                                load_time timestamp,
                                source_file string)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_kafka_multiplex_bz(self):
        if self.initialized:
            print(f"Creating Kafka Multiplex table ----->", end="")
            spark.sql(
                f""" create table if not exists {self.catalog}.{self.db_name}.kafka_multiplex_bz(
                                key string,
                                value string,
                                topic string,
                                partition bigint,
                                offset bigint,
                                timestamp bigint,
                                date date,
                                week_part string,
                                load_time timestamp,
                                source_file string)
                                partitioned by (topic, week_part)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_users(self):
        if self.initialized:
            print(f"Creating Users table ----->", end="")
            spark.sql(
                f""" create or replace table {self.catalog}.{self.db_name}.users(
                                user_id bigint,
                                device_id bigint,
                                mac_address string,
                                registration_timestamp timestamp)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_gym_logs(self):
        if self.initialized:
            print(f"Creating Gym Logs table ----->", end="")
            spark.sql(
                f""" create or replace table {self.catalog}.{self.db_name}.gym_logs(
                                mac_address string,
                                gym bigint,
                                login timestamp,
                                logout timestamp)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_user_profile(self):
        if self.initialized:
            print(f"Creating User Profile table ----->", end="")
            spark.sql(
                f""" create table if not exists {self.catalog}.{self.db_name}.user_profile(
                                user_id bigint,
                                dob date,
                                sex string,
                                gender string,
                                first_name string,
                                last_name string,
                                street_address string,
                                city string,
                                state string,
                                zip int,
                                updated timestamp)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_heart_rate(self):
        if self.initialized:
            print(f"Creating Heart Rate table ----->", end="")
            spark.sql(
                f""" create table if not exists {self.catalog}.{self.db_name}.heart_rate(
                                device_id long,
                                time timestamp,
                                heartrate double,
                                valid boolean)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_user_bins(self):
        if self.initialized:
            print(f"Creating User Bins table ----->", end="")
            spark.sql(
                f""" create table if not exists {self.catalog}.{self.db_name}.user_bins(
                                user_id bigint,
                                age string,
                                gender string,
                                city string,
                                state string)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_workouts(self):
        if self.initialized:
            print(f"Creating Workout table ----->", end="")
            spark.sql(
                f""" create table if not exists {self.catalog}.{self.db_name}.workouts(
                                user_id bigint,
                                workout_id int,
                                time timestamp,
                                action string,
                                session_id int)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_completed_workouts(self):
        if self.initialized:
            print(f"Creating Completed Workout table ----->", end="")
            spark.sql(
                f""" create table if not exists {self.catalog}.{self.db_name}.completed_workouts(
                                user_id bigint,
                                workout_id int,
                                session_id int,
                                start_time timestamp,
                                end_time timestamp)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_workout_bpm(self):
        if self.initialized:
            print(f"Creating Workout BPM table ----->", end="")
            spark.sql(
                f""" create table if not exists {self.catalog}.{self.db_name}.workout_bpm(
                                user_id bigint,
                                workout_id int,
                                session_id int,
                                start_time timestamp,
                                end_time timestamp,
                                heartrate double)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_date_lookup(self):
        if self.initialized:
            print(f"Creating Date Lookup table ----->", end="")
            spark.sql(
                f""" create table if not exists {self.catalog}.{self.db_name}.date_lookup(
                                date date,
                                week int,
                                year int,
                                month int,
                                dayofweek int,
                                dayofmonth int,
                                dayofyear int,
                                week_part string)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_workout_bpm_summary(self):
        if self.initialized:
            print(f"Creating Workout BPM Summary table ----->", end="")
            spark.sql(
                f""" create table if not exists {self.catalog}.{self.db_name}.workout_bpm_summary(
                                workout_id int,
                                session_id int,
                                user_id bigint,
                                age string,
                                gender string,
                                city string,
                                state string,
                                min_bpm double,
                                avg_bpm double,
                                max_bpm double,
                                num_recordings bigint)""")
            print("Done")
        else:
            raise ReferenceError("Application DB Is Not Defined and Cannot Create Table in default DB")

    def create_gym_summary(self):
        if self.initialized:
            print(f"Creating Gym Summary gold view...", end='')
            spark.sql(f"""CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS
                            SELECT to_date(login) date,
                            gym, l.mac_address, workout_id, session_id, 
                            round((logout::long - login::long)/60,2) minutes_in_gym,
                            round((end_time::long - start_time::long)/60,2) minutes_exercising
                            FROM gym_logs l 
                            JOIN (
                            SELECT mac_address, workout_id, session_id, start_time, end_time
                            FROM completed_workouts w INNER JOIN users u ON w.user_id = u.user_id) w
                            ON l.mac_address = w.mac_address 
                            AND w.start_time BETWEEN l.login AND l.logout
                            order by date, gym, l.mac_address, session_id""")
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.create_db()
        self.create_registered_users_bz()
        self.create_gym_login_bz()
        self.create_kafka_multiplex_bz()
        self.create_users()
        self.create_gym_logs()
        self.create_user_profile()
        self.create_heart_rate()
        self.create_workouts()
        self.create_completed_workouts()
        self.create_workout_bpm()
        self.create_user_bins()
        self.create_date_lookup()
        self.create_workout_bpm_summary()
        self.create_gym_summary()
        print(f"Setup completed in {int(time.time()) - start} seconds")

    def assert_table(self, table_name):
        assert spark.sql(f"SHOW TABLES IN {self.catalog}.{self.db_name}") \
                   .filter(f"isTemporary == false and tableName == '{table_name}'") \
                   .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table in {self.catalog}.{self.db_name}: Success")

    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        assert spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName == '{self.db_name}'") \
                    .count() == 1, f"The database '{self.catalog}.{self.db_name}' is missing"
        print(f"Found database {self.catalog}.{self.db_name}: Success")
        self.assert_table("registered_users_bz")
        self.assert_table("gym_login_bz")
        self.assert_table("kafka_multiplex_bz")
        self.assert_table("users")
        self.assert_table("gym_logs")
        self.assert_table("user_profile")
        self.assert_table("heart_rate")
        self.assert_table("workouts")
        self.assert_table("completed_workouts")
        self.assert_table("workout_bpm")
        self.assert_table("user_bins")
        self.assert_table("date_lookup")
        self.assert_table("workout_bpm_summary")
        self.assert_table("gym_summary")
        print(f"Setup validation completed in {int(time.time()) - start} seconds")

    def cleanup(self):
        if spark.sql(f"SHOW DATABASES IN {self.catalog}").filter(f"databaseName == '{self.db_name}'").count() == 1:
            print(f"Dropping the database {self.catalog}.{self.db_name}...", end='')
            spark.sql(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")
            print("Done")
        print(f"Deleting {self.landing_zone}...", end='')
        dbutils.fs.rm(self.landing_zone, True)
        print("Done")
        print(f"Deleting {self.checkpoint_base}...", end='')
        dbutils.fs.rm(self.checkpoint_base, True)
        print("Done")
