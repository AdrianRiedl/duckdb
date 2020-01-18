
#   EXAMPLEOUTPUT:
#
#    9/10...Building took 0.00237414
#    Probing took 0.00511172
#    0.006592


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
PROG_NAME = b'./../build/release/benchmark/benchmark_runner RadixJoin'


BUILDHT = "Building took"
PROBEHT = "Probing took"

numeric_const_pattern = '[-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'


class DataSaver:

    def __init__(self):
        self.elements = 3
        self.buildinghts = []                   #0
        self.probinghts = []                    #1
        self.complete = []                      #2


    def getData(self, index):
        if index == 0:
            return self.buildinghts
        if index == 1:
            return self.probinghts
        if index == 2:
            return self.complete
        if index >= 3:
            print("error - index to large getData")

    def setValue(self, index, value):
        if index == 0:
            self.buildinghts.append(value)
        if index == 1:
            self.probinghts.append(value)
        if index == 2:
            self.complete.append(value)
        if index >= 3:
            print("error - index to large getData")


    def replaceList(self, index, list):
        if index == 0:
            self.buildinghts = list
        if index == 1:
            self.probinghts = list
        if index == 2:
            self.complete = list
        if index >= 3:
            print("error - index to large getData")

    def getIndex(self, line):
        if BUILDHT in line:
            return 0
        if PROBEHT in line:
            return 1
        # has to be last!!!!!!!!!!!!!
        if "." in line:
            return 2
        else:
            print("Should not happen")
            return None

    def shrinkBuild(self):
        RUNNUMBER = 10
        lengthbuildingForKeys = len(self.buildingForKeys)
        lengthbuildingForValues = len(self.buildingForValues)
        lengthprobingForKeys = len(self.probingForKeys)
        lengthprobingAndBuildingForTuple = len(self.probingAndBuildingForTuple)
        lengthappending = len(self.appending)
        lengthbucketsearch = len(self.bucketsearch)
        if not lengthbuildingForKeys ==lengthbuildingForValues:
            print("ERROR in lenght 1")
            return
        if not lengthbuildingForValues ==lengthprobingForKeys:
            print("ERROR in lenght 2")
            return
        if not lengthprobingForKeys ==lengthprobingAndBuildingForTuple:
            print("ERROR in length 3")
            return
        if not lengthprobingAndBuildingForTuple ==lengthappending:
            print("ERROR in length 4")
            return
        if not lengthbucketsearch == lengthappending:
            print("ERROR in length 5")
            return
        partsPerRun = lengthappending / RUNNUMBER
        partsPerRun = int(partsPerRun)

        for list in self.specialcases:
            newList = []
            sum = 0
            for i in range (0, lengthappending):
                if self.getData(list)[i] == None:
                    newList.append(sum)
                    sum = 0
                    continue
                sum += self.getData(list)[i]
            self.replaceList(list, newList)


    def insertData(self, linesStderr):
        for line in linesStderr:
            if 'Killed' in line:
                break
            index = self.getIndex(line)

            rx = re.compile(numeric_const_pattern, re.VERBOSE)
            res = rx.findall(line)
            #print(res)
            res = list(map(float, res))
            if index == 0:
                self.setValue(index, res[2])
            else:
                if res[0] != 0.0:
                    self.setValue(index, res[0])



def makeAvg(list, number):
    sum = 0
    for l in list:
        sum += l
    if number == 0:
        return None
    avg = sum / number
    return avg

def saveDataAndPlots(pathStart, dataObj):
    # create the path if necessary
    if not os.path.exists(pathStart):
        os.makedirs(pathStart)
    pathDataRuntime = pathStart + b'/data_runtimeTimer.csv'
    fDataRuntime = open(pathDataRuntime, 'a+')
    all = []
    if DEBUG:
        print(dataObj.complete)
        print(dataObj.runtime)
    fileCpp = open('../../benchmark/micro/radixjoin.cpp', 'r')
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
        if avg == None:
            avg = 0
        d.append(avg)
        
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
    popen = sp.Popen(["../../build/release/benchmark/benchmark_runner", "RadixJoin"], stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines=True)

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
