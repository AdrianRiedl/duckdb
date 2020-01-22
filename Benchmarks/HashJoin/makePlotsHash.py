import os
import time

DEBUG = False
DEFINE = '#define RADIXJOIN_COUNT (size_t) {}*1024\n'
HEADINGSTIMER = ["Tuples", "BuildHT", "ProbeHT", "Runtime"]

START = 6
END = 21

def power2(ex):
    if ex == 0:
        return 1
    else:
        return 2*power2(ex-1)
        
def modifyFileDuckDB(of):
    powerOf2 = power2(of)
    with open('../../benchmark/micro/radixjoin.cpp', 'r') as file:
        # read a list of lines into data
        data = file.readlines()
        if DEBUG:
            print(data)
    for i in range(0, len(data)):
        if '#define' in data[i]:
            if DEBUG:
                print(data[i])
            data[i] = DEFINE.format(powerOf2)
            if DEBUG:
                print(data[i])
    if DEBUG:
        print(data)
    with open('../../benchmark/micro/radixjoin.cpp', 'w') as file:
        for e in data:
            file.write(e)
    file.close()
    

pathStart = b'./plotsBenchmark'
if not os.path.exists(pathStart):
    os.makedirs(pathStart)
pathDataRuntime = pathStart + b'/data_runtimeTimer.csv'
fDataRuntime = open(pathDataRuntime, 'a+')
for i in range(0, len(HEADINGSTIMER)):
    fDataRuntime.write(HEADINGSTIMER[i])
    if i != len(HEADINGSTIMER)-1:
        fDataRuntime.write(",")
    else:
        fDataRuntime.write("\n")
fDataRuntime.close()

#modifyFileDuckDBTimer()

for i in range(START,END):
    print("Modifying to " + str(i))
    modifyFileDuckDB(i)
    # Change dir to make the new executable
    os.chdir("../../build/release/benchmark")
    # Configure and make the new executable
    os.system("make -j8")
    # Change back to the Desktop
    os.chdir("../../../Benchmarks/HashJoin")
    # Wait to cool down
    time.sleep(5) # sleep 5 seconds
    print("Starting modified " + str(i))
    # Execute the benchmarkrunner
    os.system("python3 duckdbbenchmarkTimer.py")
    # Wait to cool down
    time.sleep(5) # sleep 5 seconds
    # Change dir to make the new executable
    os.chdir("../../build/release/benchmark")
    # Configure and make the new executable
    os.system("make clean")
    # Change back to the Desktop
    os.chdir("../../../Benchmarks/HashJoin")

