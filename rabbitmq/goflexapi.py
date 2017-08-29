#!/usr/bin/env python
#author mark_purcell@ie.ibm.com

import pika
import uuid
import json

#NOTE: FOR GOFLEX OPERATIONS DONT CHANGE THE CONTENTS OF THIS FILE
#REQUEST BUG FIXES OR ENHANCEMENTS AS NECESSARY


class GoFlexAPI():
    def __init__(self, host, port, user, password, publish, subscribe):
        self.client_id = str(uuid.uuid4())
        self.publish_topic = publish
        self.subscribe_topic = subscribe + '/' + self.client_id

        self.publisher = self.connect(host, port, user, password)
        self.publisher.queue_declare(queue=self.publish_topic, durable=True)
        self.subscriber = self.connect(host, port, user, password)
        self.subscriber.queue_declare(queue=self.subscribe_topic, auto_delete=True)

    def connect(self, host, port, user, password):
        credentials = pika.PlainCredentials(user, password)
        parameters = pika.ConnectionParameters(host, port, '/', credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        return channel

    def publish(self, message, correlation):
        message['serviceRequest']['requestor']['replyTo'] = self.subscribe_topic
        message['serviceRequest']['requestor']['clientID'] = self.client_id
        message['serviceRequest']['requestor']['correlationID'] = correlation
        json_message = json.dumps(message)

        var = self.publisher.basic_publish(exchange='', routing_key=self.publish_topic, body=json_message,
                            properties=pika.BasicProperties(
                                delivery_mode = 1,
                          ))

    def receive(self, timeout, message_handler):
        for msg in self.subscriber.consume(self.subscribe_topic, inactivity_timeout=timeout):
            if msg is not None:
                method_frame, properties, body = msg
                self.subscriber.basic_ack(method_frame.delivery_tag)
                done = message_handler(body)
                if done is not None:
                    break;

