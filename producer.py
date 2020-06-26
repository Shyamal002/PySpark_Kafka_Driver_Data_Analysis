#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Description: This data creation script is part of the Sparkathon 2020 competition.
             Script generates data to mimick driver availability of an online taxi service.
             A randomized record containing columns of Driver ID, Timestamp, On Duty and Location is created, converted to json format
             and sent to the Kafka broker(s).
Prerequisite: Ensure the Kafka brokers are running before script execution.
Usage: python producer.py --kafkatopicname <String> [--seed <int>] [--duration <int>] [--records <int>] [--start_time <int>] [--drivers <int>] [--kafkabootstrapserver <String>]
@authors:   Shyamal Akruvala
            
version 1.1
"""
import random
import json
import time
from collections import OrderedDict
import argparse
from kafka import KafkaProducer
import sys
import os

print("  _____       _                  _____        _           _____                           _   _            ") 
print("|  __ \     (_)                |  __ \      | |         / ____|                         | | (_)            ")
print("| |  | |_ __ ___   _____ _ __  | |  | | __ _| |_ __ _  | |  __  ___ _ __   ___ _ __ __ _| |_ _  ___  _ __  ")
print("| |  | | '__| \ \ / / _ \ '__| | |  | |/ _` | __/ _` | | | |_ |/ _ \ '_ \ / _ \ '__/ _` | __| |/ _ \| '_ \ ")
print("| |__| | |  | |\ V /  __/ |    | |__| | (_| | || (_| | | |__| |  __/ | | |  __/ | | (_| | |_| | (_) | | | |")
print("|_____/|_|  |_| \_/ \___|_|    |_____/ \__,_|\__\__,_|  \_____|\___|_| |_|\___|_|  \__,_|\__|_|\___/|_| |_|")
                                                                                                                                                                                                                
class Generator(object):
    """
    The Generator class contains the methods to generate randomized driver related data
    pertaining to their IDs, Timestamp, On Duty Availability and Location. It also includes
    methods to convert this random data into json records and push them to Kafka producers.
    """
    def __init__(self, drivers, start_time, records):
        self.data = OrderedDict()
        self.drivers = drivers
        self.start_time = start_time
        self.records = records

    def generate_driver_id(self):
        """
        The generate_driver_id method generates randomized IDs using the randint method.
        The method also includes logic to generate null values for IDs to simulate "bad" records
        which will be caught and processed accordingly in the downstream spark code.
        """
        if random.random() < 0.04:
            driver_id = None
        elif random.random() < 0.91:
            driver_id = random.randint(12471928, 12471928 + self.drivers - 1)
        else:
            driver_id = random.randint(-289759827337, -107678768)
        return {'driver_id': driver_id}

    def generate_timestamp(self):
        """
        The generate_timestamp method generates randomized timestamps using the randint method.
        """
        timestamp = self.start_time
        if random.random() < 0.05:
            timestamp += random.randint(-10000, 10000)
        data = {'timestamp': timestamp}
        return data

    def generate_on_duty(self):
        """
        The generate_on_duty method generates randomized data for on duty availability.
        """
        on_duty = 1

        # 20% chance drivers are off duty
        if random.random() < 0.2:
            on_duty = 0
        elif random.random() < 0.06:
            on_duty = random.randint(-10000, 10000)
        else:
            on_duty = 1
        return {'on_duty': on_duty}


    def generate_location(self):   
        """
        The generate_location method generates randomized latitudes and longitudes using the randint method.
        """
        offset=0
        # generate random location 
        if random.random() < 0.35:
            offset = 4
        elif random.random() < 0.25:
            offset = 5
        else:
            offset = 6    
        lat  = round(random.uniform(10, 25), offset)
        lon = round(random.uniform(70, 85), offset)
        return {'location' : str(lat) + ' ' + str(lon)}


    def get_json(self):
        """
        The get_json method creates a list of randomized driver IDs, timestamp, on duty availability and location 
        and then shuffles them to create a large dataset. The column order is shuffled. This is then converted 
        and returned as json
        """
        generate_list = (
            [self.generate_driver_id, self.generate_timestamp, self.generate_on_duty, self.generate_location]
        )
        random.shuffle(generate_list)
        for func in generate_list:
            self.data.update(func())
        return json.dumps(self.data)

    def start(self):
        """
        The start method is where the kafka producer(s) are created and randomized json data is sent.
        A counter records the number of records being sent.
        
        """
        # Initialize a Kafka Producer to consume json data       
        producer = KafkaProducer(
                                    bootstrap_servers=[args.kafkabootstrapserver], \
                                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                                )
        count = 0 # Initialize a counter to count the number of records being sent to Kafka
        while count < self.records:
            # Send data to the Kafka topic
            data=self.get_json()
            json_data=json.loads(data)
            print (json_data) # You can uncomment for debugging purpose
            producer.send(args.kafkatopicname,json_data)            
            self.data = OrderedDict()

            # Update total records
            count += 1

            prob = random.random()
            if prob <= 0.1:
                self.start_time += random.randint(-2, -1)
            elif prob <= 0.6:
                self.start_time += 1
            
            if count % 10000 == 0:
                print(str(count) + " records sent")


if __name__ == "__main__":
    """
    This is the main method where arguments are formed and passed to an instance of the Generator class
    """
    try:
        parser = argparse.ArgumentParser(
        description='Random Online Driver Data Generator')
        parser.add_argument('--seed',
                            dest='seed',
                            type=int,
                            help='seed for data generator')
        parser.add_argument('--duration',
                            dest='duration',
                            type=int,
                            help='Duration of data to generate, in seconds')
        parser.add_argument('--records',
                            dest='records',
                            type=int,
                            help='Number of records to generate')
        parser.add_argument('--start_time',
                            dest='start_time',
                            type=int,
                            help='Starting epoch time for the generated records')
        parser.add_argument('--drivers',
                            dest='drivers',
                            type=int,
                            help='Total number of drivers')
        parser.add_argument('--kafkatopicname',
                            dest='kafkatopicname',
                            type=str, required=True,
                            help='Name of the Kafka Topic to stream data')
        parser.add_argument('--kafkabootstrapserver',
                            dest='kafkabootstrapserver',
                            type=str,
                            help='Name of the Kafka Bootstrap Server and Port')
        parser.set_defaults(seed=100,
                            records=100000,
                            start_time=int(time.time()),
                            drivers=500,
                            kafkabootstrapserver='localhost:9092')

        args = parser.parse_args()

        random.seed(args.seed)
        generator = Generator(drivers=args.drivers, 
                              start_time=args.start_time, 
                              records=args.records)
        # Start the data generate process
        generator.start()
    except KeyboardInterrupt:
        print('''Program execution interrupted.
                 Exiting data generation script.
                 Good Bye and have a nice day''')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)