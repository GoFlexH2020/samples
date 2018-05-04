#!/usr/bin/env python
#authors john.d.sheehan@ie.ibm.com, mark_purcell@ie.ibm.com

import os
import ssl
import time
import json
import threading
import datetime
import pytz

import paho.mqtt.client as mqtt


#NOTE: FOR GOFLEX OPERATIONS DONT CHANGE THE CONTENTS OF THIS FILE
#REQUEST BUG FIXES OR ENHANCEMENTS AS NECESSARY


class GoFlexMeterSubmissionAPI():
    def __init__(self, config_file):
        config = self.config_file_parse(config_file)

        client_id = config['client_id']
        self.client = mqtt.Client(client_id=client_id, clean_session=True)

        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish

        cafile = config['cafile']
        self.client.tls_set(ca_certs=cafile, certfile=None, keyfile=None, 
                             cert_reqs=ssl.CERT_REQUIRED,
                             tls_version=ssl.PROTOCOL_TLSv1_2)

        username = config['username']
        password = config['password']
        self.client.username_pw_set(username, password)

        address = config['address']

        self._condition = threading.Condition()
        self.client.connect(address, 8883)
        self.client.loop_start()

        with self._condition:
            self._condition.wait()

        self.topic = config['topic']


    def utc_offset(self, local_datetime_str, local_timezone_str, datetime_format):
        '''given a local datetime string, local timezone, and optional datetime format,
           return a utc, offset tuple, or None on failure
        '''

        try:
            local_tz_obj = pytz.timezone(local_timezone_str)
            basic_dt_obj = datetime.datetime.strptime(local_datetime_str, datetime_format)
            local_dt_obj = local_tz_obj.normalize(local_tz_obj.localize(basic_dt_obj))

            utc_dt_str = local_dt_obj.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
            offset = local_dt_obj.strftime('%z')
        except pytz.exceptions.UnknownTimeZoneError:
            print 'unknown timezone {}'.format(local_timezone_str)
            return None

        #return utc_dt_str, offset)
        return utc_dt_str

    def config_file_parse(self, jsonfile):
        #Parse json file for server connection details, return dict

        '''
        expected file contents
        {
          "client_id":"d:xxxxxx:xxxxxxx:xxxx",
          "address":"xxxxxx.messaging.internetofthings.ibmcloud.com",
          "username":"use-token-auth",
          "password":"xxxxxxxxxxxxxxxxxy",
          "topic":"iot-2/evt/xxxxx/fmt/xxx"
          "cafile":"ca filename"
        }
        '''
        with open(jsonfile) as config_file:
            config = json.load(config_file)

        expected_keys = ['client_id', 'address', 'username', 'password', 'topic', 'cafile']
        for key in expected_keys:
            if config.get(key) is None:
                raise KeyError('%s is missing' % key)

        cafile = config['cafile']
        if not os.path.isfile(cafile):
            raise IOError('certificate file not found {}'.format(cafile))

        return config

    def on_connect(self, client, userdata, flags, result):
        #callback when client connects to broker
        print 'Connected.'

        with self._condition:
            self._condition.notify()

    def on_publish(self, client, userdata, result):
        #Callback when publish attempted
        print 'Published ' + str(result)

    def publish(self, message):
        rc = self.client.publish(self.topic, message, qos=2)
        rc.wait_for_publish()

    def close(self):
        # give time for publish to complete

        self.client.loop_stop()
        self.client.disconnect()

