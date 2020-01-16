
#   EXAMPLEOUTPUT:
#
#    1/10...The whole operator took: 0.512479s!
#    Building ht took: 0
#    Probing ht took: 0
#    0.517489


from __future__ import print_function
import subprocess as sp
#import matplotlib.pyplot as plt
#import numpy as np
import os
import sys
import time
import csv
import re
DEBUG = False
STDOUT = False
SHRINKED = True
RUNS = 10
NUMBER = 4*1024*1024


WHOLEOP = "The whole operator took"

numeric_const_pattern = '[-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'


class DataSaver:

    def __init__(self):
        self.elements = 1
        self.wholeop = []                       #0



    def getData(self, index):
        if index == 0:
            return self.wholeop
        if index >= 1:
            print("error - index to large getData")

    def setValue(self, index, value):
        if index == 0:
            self.wholeop.append(value)
        if index >= 1:
            print("error - index to large getData")
            
            
    def replaceList(self, index, list):
        if index == 0:
            self.wholeop = list
        if index >= 1:
            print("error - index to large getData")
            
    def getIndex(self, line):
        if WHOLEOP in line:
            return 0
        else:
            return None
        

    def insertData(self, linesStderr):
        pos = 0
        for line in linesStderr:
            if 'Killed' in line:
                break
            #print(line)
            index = self.getIndex(line)
            if index == None:
                continue
            #print(index)
            if index == 13:
                for a in self.specialcases:
                    self.setValue(a, None)
                
            rx = re.compile(numeric_const_pattern, re.VERBOSE)
            res = rx.findall(line)
            #print(res)
            res = list(map(float, res))
            #print(res)
            if index == 0:
                self.setValue(index, res[2])
            else:
                print("This should not happen")

                

def makeAvg(list, number):
    sum = 0
    for l in list:
        sum += l
    if number == 0:
        return None
    return sum/number

def saveDataAndPlots(pathStart, dataObj):
    # create the path if necessary
    if not os.path.exists(pathStart):
        os.makedirs(pathStart)
    pathDataRuntime = pathStart + b'/data_runtimeNoTimer.csv'
    fDataRuntime = open(pathDataRuntime, 'a+')
    all = []
    if DEBUG:
        print(dataObj.complete)
        print(dataObj.runtime)
    fileCpp = open('../benchmark/micro/radixjoin.cpp', 'r')
    line = fileCpp.readline()
    while not '#define' in line:
        line = fileCpp.readline()
    splits = line.split(' ')
    NUMBER = eval(splits[2])
    fDataRuntime.write(str(NUMBER) + ",")
    d = []
    sum = 0
    for i in range(0,dataObj.elements):
        vec = dataObj.getData(i)
        avg = makeAvg(vec, len(vec))
        d.append(avg)
        
        if i == 6 or i == 7 or i == 8 or i == 9 or i == 10 or i == 13 or i == 14 or i == 15:
            continue
        sum += avg
    #d[14] = sum
    for i in range(0,dataObj.elements):
        text = "{0:.10f}".format(d[i])
        if i != dataObj.elements-1:
            fDataRuntime.write(text + ",")
        else:
            fDataRuntime.write(text+ "\n")
    fDataRuntime.close()

def makePlot(dataObj):
    pathStart = b'./plotsBenchmark'
    saveDataAndPlots(pathStart, dataObj)

def exec_test():
    print("------------------------------STARTING------------------------------")
    dataObj = DataSaver()
    popen = sp.Popen(["../build/release/benchmark/benchmark_runner", "RadixJoin"], stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines=True)
    
    linesStderr = []

    for stderr_line in iter(popen.stderr.readline, ""):
        linesStderr.append(stderr_line)

    startindex = 0
    while not "DONE" in linesStderr[startindex]:
        startindex += 1
    startindex += 1
    if DEBUG:
        print(linesStderr[startindex])
    dataObj.insertData(linesStderr[startindex:len(linesStderr)])
    print("------------------------------FINISHED------------------------------")
    
    
    #if DEBUG:
#    for i in range (0,dataObj.elements):
#        print(str(i) + ": ")
#        print(dataObj.getData(i))
    # This boils down the lists containing the data of the hashtables
    
#    for i in range (0,dataObj.elements):
#        print(str(i) + ": ")
#        print(dataObj.getData(i))
    if DEBUG:
        for i in range (0,dataObj.elements):
            print(str(i) + ": ")
            print(dataObj.getData(i))
    makePlot(dataObj)
    return 1

def main():
    exec_test()

if __name__ == '__main__':
    main()
