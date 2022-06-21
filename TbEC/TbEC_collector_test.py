'''
Author: Kay_Rick@outlook.com
Date: 2021-12-03 10:48:54
LastEditors: Kay_Rick@outlook.com
LastEditTime: 2021-12-03 10:54:15
Description:
'''
import pika
import json
import time
import zlib
import sys
sys.path.append('/home/node10/rick/cloud_test')
from setParm import dataName
from setParm import max_num_edges


username = "admin"
password = "admin"
mq = "127.0.0.1"
queue_name = "edges_queue" 
routing_key_name = "edges_queue"
exchange_name = "exp"

class Trie:
    root = {}
    END = '#'  
    bitCount = 0

    def delete_root(self):
        self.root = {}

    def insert(self, word):
        node = self.root
        for c in word:
            node = node.setdefault(c, {})
        node[self.END] = None

    def delete(self, word):  
        node = self.root
        for c in word:
            if c not in node:
                print('字典中没有不用删')
                return False
            node = node[c]
        del node['/']
        while node == {}:
            if word == '':
                return
            tmp = word[-1]
            word = word[:-1]
            node = self.root
            for c in word:
                node = node[c]
            del node[tmp]

    def search(self, word):
        node = self.root
        for c in word:
            if c not in node:
                return False
            node = node[c]
        return self.END in node

    def associate_search(self,
                         pre):  
        node = self.root
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


def transforBinaryStr_opt1(fileName, edgeNum, bitWiseTrie):
    file = open(fileName, "r")
    count = 1
    for line in file:
        nodes = line.split()
        u = nodes[0]  
        v = nodes[1]  
        u = int(u)
        v = int(v)
        uBinaryStr = "{0:b}".format(u)
        vBinaryStr = "{0:b}".format(v)
        ulength = len(uBinaryStr)
        vlength = len(vBinaryStr)
        if ulength > vlength:
            lengthestBit = ulength
        else:
            lengthestBit = vlength
        uPaddingBS = paddingBinaryStr(uBinaryStr, lengthestBit)
        vPaddingBS = paddingBinaryStr(vBinaryStr, lengthestBit)
        newBinaryBS = uPaddingBS + vPaddingBS
        bitWiseTrie.insert(newBinaryBS)

        count = count + 1
        if count > edgeNum:
            return


def paddingBinaryStr(BinaryStr, lengthestBit):
    length = len(BinaryStr)
    newBinaryStr = ""
    if length < lengthestBit:
        for i in range(lengthestBit - length):
            newBinaryStr = newBinaryStr + "0"
    return newBinaryStr + BinaryStr


def restoreEdge(strList):
    edgeList = []
    for i in range(len(strList)):
        edge_list = []
        str = strList[i]
        strLen = len(str)
        uStr = str[0:int(strLen / 2)]
        vStr = str[int(strLen / 2):strLen]
        u = int(uStr, 2)
        v = int(vStr, 2)
        edge_list.append(u)
        edge_list.append(v)
        edgeList.append(edge_list)
    print(edgeList)


def init_mq():
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(mq, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    return channel


def publish(channel, body):
    channel.basic_publish(exchange=exchange_name,
                          routing_key=routing_key_name,
                          body=body)
global bitWiseTrie


if __name__ == "__main__":
    fileName = dataName + '_' + sys.argv[1]
    mq_channel = init_mq()
    bitWiseTrie = Trie()
    count = 0
    edgeCount = 0
    data = open(fileName, "r")
    for line in data:
        node = line.split()
        u = int(node[0])
        v = int(node[1])
        uBinaryStr = "{0:b}".format(u)
        vBinaryStr = "{0:b}".format(v)
        ulength = len(uBinaryStr)
        vlength = len(vBinaryStr)
        if ulength > vlength:
            lengthestBit = ulength
        else:
            lengthestBit = vlength
        uPaddingBS = paddingBinaryStr(uBinaryStr, lengthestBit)
        vPaddingBS = paddingBinaryStr(vBinaryStr, lengthestBit)
        newBinaryBS = uPaddingBS + vPaddingBS
        count += 1
        edgeCount += 1
        bitWiseTrie.insert(newBinaryBS)

        if(count >= max_num_edges):
            count = 0
            publish(mq_channel, json.dumps(bitWiseTrie.root))
            bitWiseTrie.delete_root()
    publish(mq_channel, json.dumps(bitWiseTrie.root))
    publish(mq_channel, json.dumps("EOF"))
