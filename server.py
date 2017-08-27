#!/usr/bin/env python
#####################################################################################
# Urbanova Python Class
# Version: 1.01
# Python: 2.7.9
# Author: Nathan Scott
# Email: nathan.scott@wsu.edu
# Updated: 08/15/2017
# Copyright: Remote Sensing Laboratory | Washington State University
#####################################################################################

import hashlib
import uuid
import os
import time
import re
# import serial
import urllib3
import random
import json
import pika
import uuid
from pprint import pprint
import mysql.connector
from mysql.connector import errorcode

class Server:
    #################################################################################
    # Function: rabbitMQclient()
    # Return: n/a.
    # Application: to connect rabbitMQ system.
    # Version: 1.01
    # Updated: 08/15/2017
    #################################################################################
    def rabbitMQclient(self, username, password, ip, port):
        # parameters = pika.URLParameters('amqp://user:re$earch@35.185.228.239:5672/%2F')
        parameters = pika.URLParameters("amqp://" + username + ":" + password + "@" + ip + ":" + port + "/%2F")
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        # result = self.channel.queue_declare(queue = "normal", durable = True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(self.on_response, no_ack=False,
                                   queue=self.callback_queue)

                                   
    #################################################################################
    # Function: rabbitMQserver()
    # Return: n/a.
    # Application: to connect rabbitMQ system.
    # Version: 1.00
    # Updated: 08/02/2017
    #################################################################################
    def rabbitMQserver(self, username, password, ip):                  
        parameters = pika.ConnectionParameters(ip, credentials=pika.PlainCredentials(username, password))
        connection = pika.BlockingConnection(parameters)
        return connection
                               
                               
    #################################################################################
    # Function: on_response()
    # Return: n/a.
    # Application: RabbitMQ system.
    # Version: 1.00
    # Updated: 08/01/2017
    #################################################################################
    def on_response(self, ch, method, props, body):
            if self.corr_id == props.correlation_id:
                self.response = body

    
    #################################################################################
    # Function: call()
    # Return: a string.
    # Application: to receive a data package from a queue.
    # Version: 1.00
    # Updated: 08/01/2017
    #################################################################################
    def call(self, input, q):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange = '',
                                   routing_key = q,
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=str(input))
        while self.response is None:
            self.connection.process_data_events()
        return str(self.response)

        
    #################################################################################
    # Function: conn()
    # Return: connection type object.
    # Application: to establish a connection between this device to a MYSQL database.
    # Version: 1.01
    # Updated: 07/24/2017
    #################################################################################
    def conn(self, username, password, ip, database):
        try:
            conn = mysql.connector.connect(user=username,
                                           passwd=password,
                                           host=ip,
                                           db=database)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Username or password is wrong!")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database or table does not exist!")
            else:
                print(err)
        else:
            return conn


    #################################################################################
    # Function: readMeta()
    # Return: a string.
    # Application: to read a configuration from a local file.
    # Version: 1.00
    # Updated: 07/22/2017
    #################################################################################
    def readMeta(self, filepath):
        with open(filepath) as f:    
            data = json.load(f)
            print("[ Meta ] " + repr(data) + "\n")
            # pprint(repr(data))

        f.close()
        return data


    #################################################################################
    # Function: genSig()
    # Return: a 128 bits hex string.
    # Application: to generate a string based on MD5 algorithm.
    # Version: 1.00
    # Updated: 07/22/2017
    #################################################################################
    def genSig(self,salt, meta):
        code = meta["sensor1"][0]["name"] + meta["sensor1"][0]["sn"] + meta["sensor1"][0]["calibration"] + \
               meta["sensor2"][0]["name"] + meta["sensor2"][0]["sn"] + meta["sensor2"][0]["calibration"] + \
               meta["sensor3"][0]["name"] + meta["sensor3"][0]["sn"] + meta["sensor3"][0]["calibration"] + \
               meta["sensor4"][0]["name"] + meta["sensor4"][0]["sn"] + meta["sensor4"][0]["calibration"] + \
               meta["sensor5"][0]["name"] + meta["sensor5"][0]["sn"] + meta["sensor5"][0]["calibration"] + \
               meta["sensor6"][0]["name"] + meta["sensor6"][0]["sn"] + meta["sensor6"][0]["calibration"]
        # print(repr(sn))
        # based on current data field design, md5/sha1 both ok.
        return str(hashlib.md5(salt.encode() + code.encode()).hexdigest())


    #################################################################################
    # Function: checkSig()
    # Return: True/False.
    # Application: to check if signature already exist in the database or not.
    # Version: 1.00
    # Updated: 07/22/2017
    #################################################################################
    def checkSig(self, conn, stationid, sig):
        try:
            sql = """SELECT sig FROM meta WHERE stationid = '%s' AND sig = '%s' ORDER BY mid DESC LIMIT 1""" % (stationid, sig)
            print("[ Query ] " + repr(sql))
            
            cur = conn.cursor()
            cur.execute(sql)
            result = cur.fetchone()
            conn.commit()
            print("[ Check Signature ] " + repr(result) + "\n")
            
            if result == None:
                print("[ Signature ] " + sig + " does not exist in the database!")
                return False
            
            if result[0] == sig:
                print("[ Signature ] " + result[0] + " found in the database!")
                return True

        except mysql.connector.Error as err:
            conn.rollback()
            return False


    #################################################################################
    # Function: insertData()
    # Return: True/False.
    # Application: to insert a data record into a database.
    # Version: 1.00
    # Updated: 07/22/2017
    #################################################################################
    def insertData(self, conn, table, field, value):
        try:
            sql = """INSERT INTO %s (%s) VALUES(%s)""" % (table, field, value)
            # print(repr(sql))
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            return True
        except mysql.connector.Error as err:
            print(err)
            conn.rollback()
            return False


    #################################################################################
    # Function: updateData()
    # Return: True/False.
    # Application: to update a data record into a database.
    # Version: 1.00
    # Updated: 07/24/2017
    #################################################################################
    def updateData(self, conn, mid):
        try:
            sql = """UPDATE meta SET dtime='%s' WHERE mid='%s'""" % (time.strftime("%Y-%m-%d") + " " + time.strftime("%H:%M:%S"), mid)
            print(repr(sql))
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        except mysql.connector.Error as err:
            print(err)
            conn.rollback()
            return False


    #################################################################################
    # Function: lastRecord()
    # Return: True/False.
    # Application: to find last record that accosiet with stationid in database.
    # Version: 1.00
    # Updated: 07/22/2017
    #################################################################################
    def lastRecord(self, conn, stationid):
        try:
            sql = """SELECT mid,stationid FROM meta WHERE stationid = '%s' ORDER BY mid DESC LIMIT 1""" % stationid
            print("[ Query ] " + repr(sql))

            cur = conn.cursor()
            cur.execute(sql)
            result = cur.fetchone()
            conn.commit()
            # print(type(stationid))
            # print(stationid)
            # print(type(result[1]))
            # print(result[1])

            if result == None:
                print("[ Meta ID ] " + stationid + " does not exist in the database!")
                return False
            
            if result[1] == int(stationid):
                print("[ Meta ID ] " + str(result[0]) + " found in the database!")
                return result[0]
            
        except mysql.connector.Error as err:
            print(err)
            conn.rollback()
            return False
                    
                    
    #################################################################################
    # Function: mysql()
    # Return: n/a.
    # Application: to populate data into a mysql database.
    # Version: 1.00
    # Updated: 08/02/2017
    #################################################################################
    def mysql(self, conf, data):
        try:
            conn = self.conn(conf["mysqlUsername"], conf["mysqlPassword"], conf["mysqlIP"], conf["mysqlDatabase"])
            # print(conn)

            data = data.split(",")
            # print(data)
            # print(data[2:])

            tag = data[0]
            
            if tag == "meta":
                lastRecord = data[1]
                sig = data[2]
                stationid = data[3]
                
                table = conf["mysqlMetaTable"]
                field = conf["mysqlMetaField"]
                value = str(data[2:])
                # value = value.replace("'", "")
                value = value.replace("[", "")
                value = value.replace("]", "")
                # print(value + "\n")
                
                insertData = self.insertData(conn, table, field, value)
                # print(repr(insertData) + "\n")

                updateData = self.updateData(conn, lastRecord)
                # print(repr(updateData) + "\n")
                
            if tag == "data":
                table = conf["mysqlDataTable"]
                field = conf["mysqlDataField"]
                value = str(data[1:])
                # value = value.replace("'", "")
                value = value.replace("[", "")
                value = value.replace("]", "")
                # print(value + "\n")
                
                self.insertData(conn, table, field, value)
                
            # print("DONE\n")
            conn.close()
            
        except mysql.connector.Error as err:
            print(err)
            
            
    #################################################################################
    # Function: run()
    # Return: n/a.
    # Application: to execute the program.
    # Version: 1.02
    # Updated: 08/15/2017
    #################################################################################
    def run(self, conf):
        try:
            # data = "data,1000" + str(random.randint(1,9)) + \
            #       "," + time.strftime("%Y%m%d") + \
            #       "," + time.strftime("%H%M%S")
            # self.mysql(conf, data)
            
            rabbitMQconnection = self.rabbitMQserver(conf["rabbitMQusername"],conf["rabbitMQpassword"],conf["rabbitMQip"])
            channel = rabbitMQconnection.channel()
            # print(channel)
            # print("\n")
            
            # channel.queue_declare(queue=conf["rabbitMQqueue"], durable = True)
            channel.queue_declare(queue=conf["rabbitMQqueue"])
            
            print " [*] Awaiting client...\n"

            def on_request(ch, method, props, body):
                # call mysql function
                self.mysql(conf, body)
                
                print (" [R] Received: %s"  % str(body))
                response = str(time.strftime("%Y%m%d-%H%M%S", time.localtime())) + "-" + str(random.randint(10000, 99999))
                print (" [S] Confirmation#: %s\n" % response)
                
                ch.basic_publish(exchange = '', routing_key = props.reply_to, properties = pika.BasicProperties(correlation_id = props.correlation_id),body = response)
                ch.basic_ack(delivery_tag = method.delivery_tag)
                
                print "------------------------------------------------------------\n"
                print " [*] Awaiting client...\n"

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(on_request, queue=conf["rabbitMQqueue"])
            channel.start_consuming()
            
        except BaseException as err:
            print(err)

    
#####################################################################################
#####################################################################################
