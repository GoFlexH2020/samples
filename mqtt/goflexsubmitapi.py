#!/usr/bin/env python
#authors john.d.sheehan@ie.ibm.com, mark_purcell@ie.ibm.com

import os
import ssl
import time
import json

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
        self.client.connect(address, 8883)

        self.topic = config['topic']
        self.sent = 0
        self.ackd = 0

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
        print 'connected ' + str(result)

    def on_publish(self, client, userdata, result):
        #Callback when publish attempted
        self.ackd += 1
        print 'published ' + str(result)

    def publish(self, message):
        rc = self.client.publish(self.topic, message)
        rc.wait_for_publish()
        self.sent += 1

    def close(self, wait=5):
        # give time for publish to complete
        #if self.sent != self.ackd:
        time.sleep(wait)
        self.client.disconnect()

