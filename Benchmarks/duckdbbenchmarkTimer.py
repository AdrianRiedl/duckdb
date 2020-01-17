
#   EXAMPLEOUTPUT:
#
#    10/10...Collecting the left side took: 0.0591818s!
#    Partitioning left side took: 0.0939809s!
#    Collecting the right side took: 0.0564693s!
#    Partitioning right side took: 0.0564693s!
#    Setting the partitions took: 3.49e-07ns!
#    Setting the reduced partitions took: 1.187e-06ns!
#  REPEATED MULTIPLE TIMES BEGIN
#    Building for the keys took 0.0336899s!
#    Building for the values took 0.0346425s!
#    Probing for the keys took 0.113988s!
#    Probing and building for the tuple took 0.022144s!
#    Appending took 0.0216747s!
#  REPEATED MULTIPLE TIMES END
#    Building for the keys took 0.0327748s!
#    Building for the values took 0.0335284s!
#    Probing for the keys took 0.0576722s!
#    Probing and building for the tuple took 0.0226737s!
#    Appending took 0.0217791s!
#    Building for the keys took 0.0327379s!
#    Building for the values took 0.0336307s!
#    Probing for the keys took 0.0578728s!
#    Probing and building for the tuple took 0.0225581s!
#    Appending took 0.022218s!
#    Building for the keys took 0.0333507s!
#    Building for the values took 0.034447s!
#    Probing for the keys took 0.0576653s!
#    Probing and building for the tuple took 0.0227711s!
#    Appending took 0.0223944s!
#    Performing build and probe took: 1.47109s!
#    The whole operator took: 1.77208s!
#    Building ht took: 0.572198
#    Probing ht took: 0.89867
#    Building for the keys took 0s!
#    Building for the values took 0s!
#    Probing for the keys took 0s!
#    Probing and building for the tuple took 0s!
#    Appending took 0s!
#    1.777077


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


COLLECTINGLEFT = "Collecting the left side took"
PARTITIONINGLEFT = "Partitioning left side took"
COLLECTINGRIGHT = "Collecting the right side took"
PARTITIONINGRIGHT = "Partitioning right side took"
SETTINGPART = "Setting the partitions took"
SETTINGREDPART = "Setting the reduced partitions took"
BUILDINGKEYS = "Building for the keys took"
BUILDINGVALUES = "Building for the values took"
PROBEKEYS = "Probing for the keys took"
PROBEANDBUILDTUPLES = "Probing and building for the tuple took"
APPENDING = "Appending took"
PERFORMBUILDANDPROBE = "Performing build and probe took"
WHOLEOP = "The whole operator took"
BUILDHT = "Building ht took"
PROBEHT = "Probing ht took"
BUCKETSEARCH = "BucketBuildSearchtime took"
EXTRACTINGVALBUILD = "Performing extractingValBuild took"
WRITINGDATABUILD = "Performing writingDataBuild took"
ORDERINGHASHBUILD = "Performing orderingHashBuild took"
GETTINGHT = "Performing gettingHashtable took"
GETTINGDCHUNK = "Performing gettingDChunk took"
EXTRACTINGVALPROBE = "Performing extractingValProbe took"
WRITINGDATAPROBE = "Performing writingDataProbe took"
ORDERINGHASHPROBE = "Performing orderingHashProbe took"
REMAINING = "Performing remaining took"

numeric_const_pattern = '[-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'


class DataSaver:

    def __init__(self):
        self.elements = 26
        self.specialcases = [6,7,8,9,10,16]
        self.collLeft = []                      #0
        self.partLeft = []                      #1
        self.collRight = []                     #2
        self.partRight = []                     #3
        self.settPart = []                      #4
        self.settRedPart = []                   #5
        self.buildingForKeys = []               #6
        self.buildingForValues = []             #7
        self.probingForKeys = []                #8
        self.probingAndBuildingForTuple = []    #9
        self.appending = []                     #10
        self.buildinghts = []                   #11
        self.probinghts = []                    #12
        self.perfBuildProbe = []                #13
        self.runtime = []                       #14
        self.complete = []                      #15
        self.bucketsearch = []                  #16
        self.extractingValBuild = []            #17
        self.writingDataBuild = []              #18
        self.orderingHashBuild = []             #19
        self.gettingHT = []                     #20
        self.gettingDChunk = []                 #21
        self.extractingValProbe = []            #22
        self.writingDataProbe = []              #23
        self.orderingHashProbe = []             #24
        self.remaining = []                     #25


    def getData(self, index):
        if index == 0:
            return self.collLeft
        if index == 1:
            return self.partLeft
        if index == 2:
            return self.collRight
        if index == 3:
            return self.partRight
        if index == 4:
            return self.settPart
        if index == 5:
            return self.settRedPart
        if index == 6:
            return self.buildingForKeys
        if index == 7:
            return self.buildingForValues
        if index == 8:
            return self.probingForKeys
        if index == 9:
            return self.probingAndBuildingForTuple
        if index == 10:
            return self.appending
        if index == 11:
            return self.buildinghts
        if index == 12:
            return self.probinghts
        if index == 13:
            return self.perfBuildProbe
        if index == 14:
            return self.runtime
        if index == 15:
            return self.complete
        if index == 16:
            return self.bucketsearch
        if index == 17:
            return self.extractingValBuild
        if index == 18:
            return self.writingDataBuild
        if index == 19:
            return self.orderingHashBuild
        if index == 20:
            return self.gettingHT
        if index == 21:
            return self.gettingDChunk
        if index == 22:
            return self.extractingValProbe
        if index == 23:
            return self.writingDataProbe
        if index == 24:
            return self.orderingHashProbe
        if index == 25:
            return self.remaining
        if index >= 26:
            print("error - index to large getData")

    def setValue(self, index, value):
        if index == 0:
            self.collLeft.append(value)
        if index == 1:
            self.partLeft.append(value)
        if index == 2:
            self.collRight.append(value)
        if index == 3:
            self.partRight.append(value)
        if index == 4:
            self.settPart.append(value)
        if index == 5:
            self.settRedPart.append(value)
        if index == 6:
            self.buildingForKeys.append(value)
        if index == 7:
            self.buildingForValues.append(value)
        if index == 8:
            self.probingForKeys.append(value)
        if index == 9:
            self.probingAndBuildingForTuple.append(value)
        if index == 10:
            self.appending.append(value)
        if index == 11:
            self.buildinghts.append(value)
        if index == 12:
            self.probinghts.append(value)
        if index == 13:
            self.perfBuildProbe.append(value)
        if index == 14:
            self.runtime.append(value)
        if index == 15:
            self.complete.append(value)
        if index == 16 :
            self.bucketsearch.append(value)
        if index == 17:
            self.extractingValBuild.append(value)
        if index == 18:
            self.writingDataBuild.append(value)
        if index == 19:
            self.orderingHashBuild.append(value)
        if index == 20:
            self.gettingHT.append(value)
        if index == 21:
            self.gettingDChunk.append(value)
        if index == 22:
            self.extractingValProbe.append(value)
        if index == 23:
            self.writingDataProbe.append(value)
        if index == 24:
            self.orderingHashProbe.append(value)
        if index == 25:
            self.remaining.append(value)
        if index >= 26:
            print("error - index to large getData")


    def replaceList(self, index, list):
        if index == 0:
            self.collLeft = list
        if index == 1:
            self.partLeft = list
        if index == 2:
            self.collRight = list
        if index == 3:
            self.partRight = list
        if index == 4:
            self.settPart = list
        if index == 5:
            self.settRedPart = list
        if index == 6:
            self.buildingForKeys = list
        if index == 7:
            self.buildingForValues = list
        if index == 8:
            self.probingForKeys = list
        if index == 9:
            self.probingAndBuildingForTuple = list
        if index == 10:
            self.appending = list
        if index == 11:
            self.buildinghts = list
        if index == 12:
            self.probinghts = list
        if index == 13:
            self.perfBuildProbe = list
        if index == 14:
            self.runtime = list
        if index == 15:
            self.complete = list
        if index == 16:
            self.bucketsearch = list
        if index == 17:
            self.extractingValBuild = list
        if index == 18:
            self.writingDataBuild = list
        if index == 19:
            self.orderingHashBuild = list
        if index == 20:
            self.gettingHT = list
        if index == 21:
            self.gettingDChunk = list
        if index == 22:
            self.extractingValProbe = list
        if index == 23:
            self.writingDataProbe = list
        if index == 24:
            self.orderingHashProbe = list
        if index == 25:
            self.remaining = list
        if index >= 26:
            print("error - index to large getData")

    def getIndex(self, line):
        if COLLECTINGLEFT in line:
            return 0
        if PARTITIONINGLEFT in line:
            return 1
        if COLLECTINGRIGHT in line:
            return 2
        if PARTITIONINGRIGHT in line:
            return 3
        if SETTINGPART in line:
            return 4
        if SETTINGREDPART in line:
            return 5
        if BUILDINGKEYS in line:
            return 6
        if BUILDINGVALUES in line:
            return 7
        if PROBEKEYS in line:
            return 8
        if PROBEANDBUILDTUPLES in line:
            return 9
        if APPENDING in line:
            return 10
        if BUILDHT in line:
            return 11
        if PROBEHT in line:
            return 12
        if PERFORMBUILDANDPROBE in line:
            return 13
        if WHOLEOP in line:
            return 14
        if BUCKETSEARCH in line:
            return 16
        if EXTRACTINGVALBUILD in line:
            return 17
        if WRITINGDATABUILD in line:
            return 18
        if ORDERINGHASHBUILD in line:
            return 19
        if GETTINGHT in line:
            return 20
        if GETTINGDCHUNK in line:
            return 21
        if EXTRACTINGVALPROBE in line:
            return 22
        if WRITINGDATAPROBE in line:
            return 23
        if ORDERINGHASHPROBE in line:
            return 24
        if REMAINING in line:
            return 25
        # has to be last!!!!!!!!!!!!!
        if "." in line:
            return 15
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
        pos = 0
        for line in linesStderr:
            if 'Killed' in line:
                break
            #print(line)
            index = self.getIndex(line)
            #print(index)
            if index == 13:
                for a in self.specialcases:
                    self.setValue(a, None)
#                self.setValue(self.getIndex(BUILDINGKEYS), None)
#                self.setValue(self.getIndex(BUILDINGVALUES), None)
#                self.setValue(self.getIndex(PROBEKEYS), None)
#                self.setValue(self.getIndex(PROBEANDBUILDTUPLES), None)
#                self.setValue(self.getIndex(APPENDING), None)
#                self.

            rx = re.compile(numeric_const_pattern, re.VERBOSE)
            res = rx.findall(line)
            #print(res)
            res = list(map(float, res))
            #print(res)
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
    return sum/number

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
        if avg == None:
            avg = 0
        d.append(avg)

        if i == 6 or i == 7 or i == 8 or i == 9 or i == 10 or i == 13 or i == 14 or i == 15:
            continue
        sum += avg
    d[14] = sum
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
    dataObj.shrinkBuild()

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
