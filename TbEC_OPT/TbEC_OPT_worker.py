'''
Author: Kay_Rick@outlook.com
Date: 2021-11-30 19:34:18
LastEditors: Kay_Rick@outlook.com
LastEditTime: 2021-12-06 21:57:26
Description:
'''
#############三角形计数Triest-IMPR算法实现
import csv
import time
from numpy import random
import json
import pika
import sys
import os
import zlib
sys.path.append('/home/node10/rick/cloud_test')
from setParm import sizeReservior

global total_worker # worker 数量
global id #该worker id


username = "admin"
password = "admin"
mq = "127.0.0.1"
global consumer_queue_name
exchange_name = "exp"
aggregator_queue_name = "aggregator_queue"
aggregator_routing_key_name = "aggregator_queue"


def init_mq():
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(mq, credentials=credentials))
    consumer_channel = connection.channel()
    # consumer_channel.confirm_delivery()
    consumer_channel.queue_declare(queue=consumer_queue_name, durable=True)
    aggregator_channel = connection.channel()
    aggregator_channel.queue_declare(queue=aggregator_queue_name, durable=True)
    # aggregator_channel.confirm_delivery()
    return consumer_channel, aggregator_channel


def publish(channel, routing_key_name, body):
    channel.basic_publish(exchange=exchange_name, routing_key=routing_key_name, body=body)


# 统计边权
def associate_search(node, pre):  # 搜索引擎里面的功能是你输入东西,不关是不是单词,他都输出以这个东西为前缀的单词.
    for c in pre:
        if c not in node:
            return []  # 因为字典里面没有pre这个前缀
        node = node[c]  # 有这个前缀就继续走,这里有个问题就是需要记录走过的路径才行.

    def travel(node):  # 返回node节点和他子节点拼出的所有单词
        if node == None:
            return ['']
        a = []  # 现在node是/ef

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

def restoreEdge(strList):
    edgeList = []
    for i in range(len(strList)):
        edge_list = []
        # print(strList[i])
        str = strList[i]
        strLen = len(str)
        uStr = str[0:int(strLen/2)]
        # print("u:" + uStr)
        vStr = str[int(strLen/2):strLen]
        # print("v:" + vStr)
        u = int(uStr, 2)
        v = int(vStr, 2)
        edge_list.append(u)
        edge_list.append(v)
        edgeList.append(edge_list)
    return edgeList

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

def distribute_count(edge, aggregator_channel):
    global edge_count
    if not edge == "EOF":
        edge_list = getAllList(edge)
        edge_node = restoreEdge(edge_list)
        for edge in edge_node:
            # print(edge[0], " ", edge[1])
            edge_count += 1
            processEdeg(edge[0], edge[1])
            # print(triangle_count_global)
    else:
        res = [int(id), triangle_count_global]
        print("Recv edge count", edge_count)
        publish(aggregator_channel, aggregator_routing_key_name, json.dumps(res))
        sys.exit(0)
    # else:
    #     res = []
    #     res.append(0)
    #     res.append(triangle_count_global)
    #     publish(aggregator_channel, aggregator_routing_key_name, res)
    #     aggregator_channel.close()


def processEdeg(u, v):
    global streamLength
    readyNodes(u)
    readyNodes(v)
    weight = weight_impr()
    checkTriangles(u, v, 1, weight)

    # centralized
    # sampleEdge(u, v)

    # distributed
    f_u = u % int(total_worker)
    f_v = v % int(total_worker)
    if (f_u == int(id) or f_v == int(id)):
        streamLength = streamLength + 1
        sampleEdge(u, v)


def readyNodes(u):
    if not samples_map.__contains__(u):
        samples_map[u] = {}
    if not triangle_count_local.__contains__(u):
        triangle_count_local[u] = 0.0


def weight_impr():
    p = 1.0
    if streamLength >= 3:
        p = ((streamLength) / size_reservior) * (streamLength - 1) / (size_reservior - 1)
        p = max(p, 1.0)
    return p


def checkTriangles(u, v, sign, weight):
    global triangle_count_global
    common = calculateCommonNeighbors(u, v)
    for c in common:
        triangle_count_global += sign * weight
        triangle_count_local[c] += sign * weight
        triangle_count_local[u] += sign * weight
        triangle_count_local[v] += sign * weight


def calculateCommonNeighbors(u, v):
    u_key_list = samples_map[u].keys()
    v_key_list = samples_map[v].keys()
    common = []
    tmp = [val for val in u_key_list if val in v_key_list]
    for i in tmp:
        common.append(i)
    return common


def sampleEdge(u, v):
    if getSampledEdgeCount() < size_reservior:
        add_reservior(u, v)
    else:
        rand_num = random.random()
        thres = size_reservior / streamLength
        if rand_num < thres:
            rand_pos = random.randint(0, size_reservior - 1)
            to_remove = samples[rand_pos]
            delete_reservior(to_remove, rand_pos)
            add_reservior(u, v)


def getSampledEdgeCount():
    return len(samples)


def add_reservior(u, v):
    temp = [u, v]
    samples.append(temp)
    temp = {}
    temp[v] = 1
    samples_map[u].update(temp)
    temp = {}
    temp[u] = 1
    samples_map[v].update(temp)


def delete_reservior(to_remove, rand_pos):
    if rand_pos < size_reservior - 1:
        last_edge = samples[len(samples) - 1]
        samples[rand_pos] = last_edge
    del samples[len(samples) - 1]
    if samples_map[to_remove[0]].__contains__(to_remove[1]):
        del samples_map[to_remove[0]][to_remove[1]]
    if samples_map[to_remove[1]].__contains__(to_remove[0]):
        del samples_map[to_remove[1]][to_remove[0]]



samples = []  # 采样的边
samples_map = {}  # 采样集{u:{v:0.0}}
triangle_count_local = {}  # 局部计数结果
global triangle_count_global  # 全局计数结果
global streamLength  # 边流
global size_reservior
global edge_count  # 边流
global eof_count


def main():
    consumer_channel, aggregator_channel = init_mq()

    def callback(ch, method, properties, body):
        # print("Received edge %r" % json.loads(body))
        # body = zlib.decompress(body) #################
        distribute_count(json.loads(body), aggregator_channel)

    consumer_channel.basic_consume(queue=consumer_queue_name, on_message_callback=callback, auto_ack=True)
    consumer_channel.start_consuming()


if __name__ == "__main__":
    total_worker = sys.argv[1]
    id = sys.argv[2]
    consumer_queue_name = "worker_queue_" + id #######################################################################################
    size_reservior = sizeReservior  # 采样集大小
    triangle_count_global = 0.0  # 初始化全局计数结果
    streamLength = 0  # 初始化边流数量
    edge_count = 0
    eof_count = 0
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)