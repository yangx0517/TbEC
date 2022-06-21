'''
Author: Kay_Rick@outlook.com
Date: 2021-11-30 17:19:07
LastEditors: Kay_Rick@outlook.com
LastEditTime: 2021-12-06 21:58:15
Description:
'''
# import threading
import json
import pika
import os
import sys
sys.path.append('/home/node10/rick/cloud_test')
from setParm import trueCount

username = "admin"
password = "admin"
mq = "127.0.0.1"
consumer_queue_name = "aggregator_queue"
exchange_name = "exp"


def init_mq():
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(mq, credentials=credentials))
    consumer_channel = connection.channel()
    consumer_channel.queue_declare(queue=consumer_queue_name, durable=True)

    return consumer_channel



def main():
    consumer_channel = init_mq()

    def callback(ch, method, properties, body):
        global count
        global global_res
        res = json.loads(body)
        global_res += res[1]
        
        count += 1
        
        if count == int(total_worker):
            print(abs(global_res - trueCount)/trueCount) 
            sys.exit(0)
        

    consumer_channel.basic_consume(queue=consumer_queue_name, on_message_callback=callback, auto_ack=True)
    consumer_channel.start_consuming()


# global result
global count
global global_res
global total_worker

if __name__ == '__main__':
    try:
        total_worker = sys.argv[1]
        count = 0
        global_res = 0
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)