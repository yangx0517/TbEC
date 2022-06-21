'''
Author: Kay_Rick@outlook.com
Date: 2021-11-30 17:19:07
LastEditors: Kay_Rick@outlook.com
LastEditTime: 2021-12-03 11:17:00
Description:
'''
# import threading
import json
import pika
import os
import sys
import copy
import zlib


username = "admin"
password = "admin"
mq = "127.0.0.1"
consumer_queue_name = "edges_queue"
exchange_name = "exp"
worker_queue_name = ["worker_queue_0", "worker_queue_1", "worker_queue_2", "worker_queue_3", 
                        "worker_queue_4", "worker_queue_5", "worker_queue_6", "worker_queue_7", 
                        "worker_queue_8", "worker_queue_9"]  
worker_routing_key_name = ["worker_queue_0", "worker_queue_1", "worker_queue_2", "worker_queue_3", 
                        "worker_queue_4", "worker_queue_5", "worker_queue_6", "worker_queue_7", 
                        "worker_queue_8", "worker_queue_9"]  


def init_mq():
    worker_channel = []
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(mq, credentials=credentials))
    consumer_channel = connection.channel()
    consumer_channel.queue_declare(queue=consumer_queue_name, durable=True)

    for i in range(10): 
        channel = connection.channel()
        channel.queue_declare(queue=worker_queue_name[i], durable=True)
        worker_channel.append(channel)

    return consumer_channel, worker_channel


def publish(channel, routing_key_name, body):
    channel.basic_publish(exchange=exchange_name,
                          routing_key=routing_key_name,
                          body=body)

def associate_search(node, pre):
    for c in pre:
        if c not in node:
            return []  
        node = node[c]  

    def travel(node): 
        if node == None:
            return ['']
        a = []  

        for i in node:
            tmp = node[i]
            tmp2 = travel(tmp)
            for j in tmp2:

                a.append(i + j)
        return a

    output = travel(node)
    for i in range(len(output)):
        output[i] = (pre + output[i])[:-1]
    return output


def search(node, word):
    index = 0
    while index < len(word):
        if word[index] in node:
            node = node[word[index]]
            index = index + 1
        else:
            isFind = False
            for key in node:
                if key != '#':
                    if len(word) - index >= len(key):
                        right_bound = index + len(key)
                        sub_word = word[index:right_bound]
                        if(sub_word == key):
                            node = node[sub_word]
                            index = right_bound
                            isFind = True
                            break
            if not isFind:
                return False

    return '#' in node

def delete(trie, word):
    node = trie
    index = 0
    parent_node = node
    parent_key = ''
    while index < len(word):
        if word[index] in node:
            parent_node = node
            parent_key = word[index]
            node = node[word[index]]
            index = index + 1
        else:
            isFind = False
            for key in node:
                if key != '#':
                    if len(word) - index >= len(key):
                        right_bound = index + len(key)
                        sub_word = word[index:right_bound]
                        if(sub_word == key):
                            parent_node = node
                            parent_key = sub_word
                            node = node[sub_word]
                            index = right_bound
                            isFind = True
                            break
            if not isFind:
                return node
    if '#' in node:
        node.pop('#')
    if len(node.keys()) == 0:
        parent_node.pop(parent_key)
    return trie

def restoreEdge(node, strList):
    trie0 = copy.deepcopy(node) 
    trie1 = copy.deepcopy(node)
    trie2 = copy.deepcopy(node) 
    trie3 = copy.deepcopy(node) 
    trie4 = copy.deepcopy(node) 
    trie5 = copy.deepcopy(node)
    trie6 = copy.deepcopy(node) 
    trie7 = copy.deepcopy(node) 
    trie8 = copy.deepcopy(node)
    trie9 = copy.deepcopy(node) 

    for i in range(len(strList)):
        str = strList[i]
        strLen = len(str)
        uStr = str[0:int(strLen/2)]
        vStr = str[int(strLen/2):strLen]
        u = int(uStr, 2)
        v = int(vStr, 2)
        f_u = u % 10
        f_v = v % 10
        if (f_u == f_v and f_u == 0): 
            node = delete(node, str)
            trie1 = delete(trie1, str)
            trie2 = delete(trie2, str)
            trie3 = delete(trie3, str)
            trie4 = delete(trie4, str)
            trie5 = delete(trie5, str)
            trie6 = delete(trie6, str)
            trie7 = delete(trie7, str)
            trie8 = delete(trie8, str)
            trie9 = delete(trie9, str)
        elif (f_u == f_v and f_u == 1): 
            node = delete(node, str)
            trie0 = delete(trie0, str)
            trie2 = delete(trie2, str)
            trie3 = delete(trie3, str)
            trie4 = delete(trie4, str)
            trie5 = delete(trie5, str)
            trie6 = delete(trie6, str)
            trie7 = delete(trie7, str)
            trie8 = delete(trie8, str)
            trie9 = delete(trie9, str)
        elif (f_u == f_v and f_u == 2): 
            node = delete(node, str)
            trie0 = delete(trie0, str)
            trie1 = delete(trie1, str)
            trie3 = delete(trie3, str)
            trie4 = delete(trie4, str)
            trie5 = delete(trie5, str)
            trie6 = delete(trie6, str)
            trie7 = delete(trie7, str)
            trie8 = delete(trie8, str)
            trie9 = delete(trie9, str)
        elif (f_u == f_v and f_u == 3): 
            node = delete(node, str)
            trie0 = delete(trie0, str)
            trie1 = delete(trie1, str)
            trie2 = delete(trie2, str)
            trie4 = delete(trie4, str)
            trie5 = delete(trie5, str)
            trie6 = delete(trie6, str)
            trie7 = delete(trie7, str)
            trie8 = delete(trie8, str)
            trie9 = delete(trie9, str)
        elif (f_u == f_v and f_u == 4): 
            node = delete(node, str)
            trie0 = delete(trie0, str)
            trie1 = delete(trie1, str)
            trie2 = delete(trie2, str)
            trie3 = delete(trie3, str)
            trie5 = delete(trie5, str)
            trie6 = delete(trie6, str)
            trie7 = delete(trie7, str)
            trie8 = delete(trie8, str)
            trie9 = delete(trie9, str)
        elif (f_u == f_v and f_u == 5): 
            node = delete(node, str)
            trie0 = delete(trie0, str)
            trie1 = delete(trie1, str)
            trie2 = delete(trie2, str)
            trie3 = delete(trie3, str)
            trie4 = delete(trie4, str)
            trie6 = delete(trie6, str)
            trie7 = delete(trie7, str)
            trie8 = delete(trie8, str)
            trie9 = delete(trie9, str)
        elif (f_u == f_v and f_u == 6): 
            node = delete(node, str)
            trie0 = delete(trie0, str)
            trie1 = delete(trie1, str)
            trie2 = delete(trie2, str)
            trie3 = delete(trie3, str)
            trie4 = delete(trie4, str)
            trie5 = delete(trie5, str)
            trie7 = delete(trie7, str)
            trie8 = delete(trie8, str)
            trie9 = delete(trie9, str)
        elif (f_u == f_v and f_u == 7): 
            node = delete(node, str)
            trie0 = delete(trie0, str)
            trie1 = delete(trie1, str)
            trie2 = delete(trie2, str)
            trie3 = delete(trie3, str)
            trie4 = delete(trie4, str)
            trie5 = delete(trie5, str)
            trie6 = delete(trie6, str)
            trie8 = delete(trie8, str)
            trie9 = delete(trie9, str)
        elif (f_u == f_v and f_u == 8): 
            node = delete(node, str)
            trie0 = delete(trie0, str)
            trie1 = delete(trie1, str)
            trie2 = delete(trie2, str)
            trie3 = delete(trie3, str)
            trie4 = delete(trie4, str)
            trie5 = delete(trie5, str)
            trie6 = delete(trie6, str)
            trie7 = delete(trie7, str)
            trie9 = delete(trie9, str)
        elif (f_u == f_v and f_u == 9): 
            node = delete(node, str)
            trie0 = delete(trie0, str)
            trie1 = delete(trie1, str)
            trie2 = delete(trie2, str)
            trie3 = delete(trie3, str)
            trie4 = delete(trie4, str)
            trie5 = delete(trie5, str)
            trie6 = delete(trie6, str)
            trie7 = delete(trie7, str)
            trie8 = delete(trie8, str)
        else:
            trie0 = delete(trie0, str)
            trie1 = delete(trie1, str)
            trie2 = delete(trie2, str)
            trie3 = delete(trie3, str)
            trie4 = delete(trie4, str)
            trie5 = delete(trie5, str)
            trie6 = delete(trie6, str)
            trie7 = delete(trie7, str)
            trie8 = delete(trie8, str)
            trie9 = delete(trie9, str)
    return node, trie0, trie1, trie2, trie3, trie4, trie5, trie6, trie7, trie8, trie9


def getAllList(node):
    allList = []
    value = ''
    def transversal(node, value):
        for key in node:
            if(key != '#'):
                newValue = value + key
                transversal(node[key], newValue)
            else:
                nonlocal allList
                allList.append(value)
    
    transversal(node, value)
    return allList


def distribute_edge(edge_trie, worker_channel, consumer_channel):
    global count
    global eof_count

    if not edge_trie == "EOF":
        edge_list = getAllList(edge_trie)
        node, trie0, trie1, trie2, trie3, trie4, trie5, trie6, trie7, trie8, trie9 = restoreEdge(edge_trie, edge_list)
        if node:
            publish(worker_channel[0], worker_routing_key_name[0], json.dumps(node))
            publish(worker_channel[1], worker_routing_key_name[1], json.dumps(node))
            publish(worker_channel[2], worker_routing_key_name[2], json.dumps(node))
            publish(worker_channel[3], worker_routing_key_name[3], json.dumps(node))
            publish(worker_channel[4], worker_routing_key_name[4], json.dumps(node))
            publish(worker_channel[5], worker_routing_key_name[5], json.dumps(node))
            publish(worker_channel[6], worker_routing_key_name[6], json.dumps(node))
            publish(worker_channel[7], worker_routing_key_name[7], json.dumps(node))
            publish(worker_channel[8], worker_routing_key_name[8], json.dumps(node))
            publish(worker_channel[9], worker_routing_key_name[9], json.dumps(node))
        if trie0:
            publish(worker_channel[0], worker_routing_key_name[0], json.dumps(trie0))
        if trie1:
            publish(worker_channel[1], worker_routing_key_name[1], json.dumps(trie1))
        if trie2:
            publish(worker_channel[2], worker_routing_key_name[2], json.dumps(trie2))
        if trie3:
            publish(worker_channel[3], worker_routing_key_name[3], json.dumps(trie3))
        if trie4:
            publish(worker_channel[4], worker_routing_key_name[4], json.dumps(trie4))
        if trie5:
            publish(worker_channel[5], worker_routing_key_name[5], json.dumps(trie5))
        if trie6:
            publish(worker_channel[6], worker_routing_key_name[6], json.dumps(trie6))
        if trie7:
            publish(worker_channel[7], worker_routing_key_name[7], json.dumps(trie7))
        if trie8:
            publish(worker_channel[8], worker_routing_key_name[8], json.dumps(trie8))
        if trie9:
            publish(worker_channel[9], worker_routing_key_name[9], json.dumps(trie9))
    else:
        if eof_count == 1: 
            for i in range(10):
                publish(worker_channel[i], worker_routing_key_name[i], json.dumps(edge_trie))
                worker_channel[i].close()
            sys.exit(0)
        else:
            eof_count += 1    


global count
global eof_count

def main():

    consumer_channel, worker_channel = init_mq()

    def callback(ch, method, properties, body):
        distribute_edge(json.loads(body), worker_channel, consumer_channel)
    consumer_channel.basic_consume(queue=consumer_queue_name,
                                   on_message_callback=callback,
                                   auto_ack=True)
    consumer_channel.start_consuming()


if __name__ == '__main__':
    try:
        count = 0
        eof_count = 0
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)