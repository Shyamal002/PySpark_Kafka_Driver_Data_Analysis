#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Description: This script reads data from the Kafka topic, processes it and writes to csv files.
Prerequisite: Ensure the Kafka brokers are running before script execution.
Usage: spark-submit --jars spark-sql-kafka-0-10_2.11-2.4.0.jar,kafka-clients-1.1.0.jar,spark-sql-kafka-0-10_2.12-2.4.5.jar consumer.py --outputdirname <String> --kafkatopicname <String> [--kafkabootstrapserver <String>]
@authors:   Shyamal Akruvala
            
version 1.3
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, window, col, when, count, isnull
import argparse, time

# Creating a Spark Session
spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Sparkathon Group 3.1") \
        .getOrCreate()        

spark.conf.set("spark.streaming.stopGracefullyOnShutdown","true")
spark.sparkContext.setLogLevel('ERROR')

parser = argparse.ArgumentParser(description='Online Driver Data Processing Program')
parser.add_argument('--kafkabootstrapserver', dest='kafkabootstrapserver', type=str, required=False, help='Kafka Bootstrap Server details and port')
parser.add_argument('--kafkatopicname', dest='kafkatopicname', type=str, required=True, help='Kafka topic to read the data')
parser.add_argument('--outputdirname', dest='outputdirname', type=str, required=True, help='Output directory to write data files')
parser.set_defaults(kafkabootstrapserver='localhost:9092')
args = parser.parse_args()

def foreachBatch_Driver_Record(df, epoch_id):
    '''
    This method is applied for each micro batch and calculate the online availability of drivers.
    Data is deduplicated and aggregated.
    '''
    print('In Driver Record Method -> Epoch Id:',epoch_id)
    df_sorted = df.sort("epoch_time",ascending=False)
    df_deduped = df_sorted.dropDuplicates(['minute', 'driver_id'])
    aggregate_df = df_deduped.groupBy(df_deduped.minute) \
                             .agg({'driver_id': 'count','on_duty':'sum'}) \
                             .sort(df_deduped.minute)
    final_result = aggregate_df.withColumnRenamed("minute", "ts_time") \
                               .withColumnRenamed("count(driver_id)", "online_driver") \
                               .withColumnRenamed("sum(on_duty)", "available_driver")
    #final_result.show()
    final_result.orderBy("minute").coalesce(1).write.csv(args.outputdirname + 'online\\' + str(int(round(time.time() * 1000))), sep=",")#, header=True)
    pass

def foreachBatch_Audit_Record(df, epoch_id):
    '''
    This method is applied for each micro batch of the audit dataframes. Data is aggregated to fetch the relevant counts.
    '''
    print('In Audit Record Method -> Epoch Id:',epoch_id)
    df_audit_sorted = df.sort("ts_minute",ascending=False)
    df_audit_deduped = df_audit_sorted.dropDuplicates(['ts_minute', 'driver_id'])
    df_audit_aggregate = df_audit_deduped.groupBy(df_audit_deduped.ts_minute) \
                             .agg(count('*').alias('total_record_received') \
                                 ,count('driver_id').alias('total_record_received_without_null') \
                                 ,count('total_record_received_with_null').alias('total_record_received_with_null'))
    #df_audit_aggregate.show()
    df_audit_aggregate.orderBy("ts_minute").coalesce(1).write.csv(args.outputdirname + 'audit\\' + str(int(round(time.time() * 1000))), sep=",") #, header=True)
    pass

if __name__ == "__main__": 

    print("  _____        _____                  _      _  __      __ _            _____ _                   _                      _    _____ _                            _             ")
    print(" |  __ \      / ____|                | |    | |/ /     / _| |          / ____| |                 | |                    | |  / ____| |                          (_)            ")
    print(" | |__) |   _| (___  _ __   __ _ _ __| | __ | ' / __ _| |_| | ____ _  | (___ | |_ _ __ _   _  ___| |_ _   _ _ __ ___  __| | | (___ | |_ _ __ ___  __ _ _ __ ___  _ _ __   __ _ ")
    print(" |  ___/ | | |\___ \| '_ \ / _` | '__| |/ / |  < / _` |  _| |/ / _` |  \___ \| __| '__| | | |/ __| __| | | | '__/ _ \/ _` |  \___ \| __| '__/ _ \/ _` | '_ ` _ \| | '_ \ / _` |")
    print(" | |   | |_| |____) | |_) | (_| | |  |   <  | . \ (_| | | |   < (_| |  ____) | |_| |  | |_| | (__| |_| |_| | | |  __/ (_| |  ____) | |_| | |  __/ (_| | | | | | | | | | | (_| |")
    print(" |_|    \__, |_____/| .__/ \__,_|_|  |_|\_\ |_|\_\__,_|_| |_|\_\__,_| |_____/ \__|_|   \__,_|\___|\__|\__,_|_|  \___|\__,_| |_____/ \__|_|  \___|\__,_|_| |_| |_|_|_| |_|\__, |")
    print("         __/ |      | |                                                                                                                                                   __/ |")
    print("        |___/       |_|                                                                                                                                                  |___/ ")

    # Reading incoming records from kafka topic
    sdfDrivers = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers",args.kafkabootstrapserver) \
        .option("subscribe", args.kafkatopicname) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss","false") \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    print("The streaming application has started reading from the Kafka topic " + args.kafkatopicname + " ................")

    # Creating and applying a schema with desired columns and datatypes
    schema = StructType(
                        [StructField("driver_id", IntegerType()),
                         StructField("timestamp", IntegerType()),
                         StructField("on_duty", ShortType()),
                         StructField("location", StringType())
                        ])
    df_driver = sdfDrivers.select(from_json(sdfDrivers.value, schema).alias("driver"))
    # df_driver.printSchema()
    df_filterDriver = df_driver.selectExpr("driver.driver_id", "driver.timestamp", "driver.on_duty", "driver.location")
    
    # Data type checks
    df_filterDriverCast = df_filterDriver.selectExpr("CAST(driver_id as INTEGER) AS driver_id"
                                                    ,"from_unixtime(timestamp) AS timestamp"
                                                    ,"CAST(timestamp as INTEGER) AS epoch_time"
                                                    ,"on_duty"                                                 
                                                    ,"location")   
    # df_filterDriverCast.printSchema()

    # Creating a temporary view to be used in SQL
    df_filterDriverCast.createOrReplaceTempView("driver")
    
    ## Audit trail view
    audit_view = spark.sql("SELECT DATE_FORMAT(timestamp,'yyyy-MM-dd HH:mm:00') AS ts_minute \
                            ,driver_id \
                            ,CASE WHEN driver_id IS NULL THEN 1 END AS total_record_received_with_null \
                            FROM driver")     

    # audit_view.printSchema()

    ## Online driver view showing availability per minute. Dropping any null records
    online_driver_view = spark.sql("select \
                                    DATE_FORMAT(timestamp,'yyyy-MM-dd HH:mm:00') AS minute \
                                    ,epoch_time \
                                    ,driver_id \
                                    ,on_duty \
                                    from driver").na.drop()
    # online_driver_view.printSchema()

    ## Raw driver level data view showing the incoming data as is without any null records
    raw_driver_view = spark.sql("select \
                                 DATE_FORMAT(timestamp,'yyyy-MM-dd HH:mm:00') AS ts_minute \
                                 ,epoch_time \
                                 ,driver_id \
                                 ,on_duty \
                                 ,location \
                                 from driver").na.drop()
    #raw_driver_view.printSchema()


### The below lines can be used to print output to Console for debugging purpose
    online_driver_view.writeStream \
       .outputMode('append') \
       .format("console") \
       .trigger(processingTime='1 minute') \
       .foreachBatch(foreachBatch_Driver_Record) \
       .start()

    audit_view \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .trigger(processingTime='1 minute') \
       .foreachBatch(foreachBatch_Audit_Record) \
       .start()

### Write data to CSV files.  
    raw_driver_view.writeStream \
                    .outputMode("append") \
                    .format("csv") \
                    .option("path", args.outputdirname + "raw\\") \
                    .option("checkpointLocation", args.outputdirname + "raw\\") \
                    .start() 

    # online_driver_view.writeStream \
    #                 .outputMode("append") \
    #                 .trigger(processingTime='1 minute') \
    #                 .foreachBatch(foreachBatch_Driver_Record) \
    #                 .format("csv") \
    #                 .option("path", "C:\\Sparkathon\\data\\online") \
    #                 .option("checkpointLocation", "C:\\Sparkathon\\data\\online") \
    #                 .start() 

    # audit_view.writeStream \
    #                 .outputMode("append") \
    #                 .trigger(processingTime='1 minute') \
    #                 .foreachBatch(foreachBatch_Audit_Record) \
    #                 .format("csv") \
    #                 .option("path", "C:\\Sparkathon\\data\\audit") \
    #                 .option("checkpointLocation", "C:\\Sparkathon\\data\\audit") \
    #                 .start()

spark.streams.awaitAnyTermination()  # Keep the program running until any of the streams terminate
print("Program execution interrupted. Exiting the streaming application.")
print("Good Bye and have a nice day")
spark.stop()