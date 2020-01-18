import os
import time

DEBUG = False

RADIXJOINEX = '#define RADIX_JOIN_EX {}\n'

def modifyFileDuckDBRadixExOn():
    with open('../src/include/duckdb/planner/operator/logical_comparison_join.hpp', 'r') as file:
        # read a list of lines into data
        data = file.readlines()
        if DEBUG:
            print(data)
    for i in range(0, len(data)):
        if '#define' in data[i]:
            if DEBUG:
                print(data[i])
            data[i] = RADIXJOINEX.format(1)
            if DEBUG:
                print(data[i])
    if DEBUG:
        print(data)
    with open('../src/include/duckdb/planner/operator/logical_comparison_join.hpp', 'w') as file:
        for e in data:
            file.write(e)
    file.close()
    
    
def modifyFileDuckDBRadixExOff():
    with open('../src/include/duckdb/planner/operator/logical_comparison_join.hpp', 'r') as file:
        # read a list of lines into data
        data = file.readlines()
        if DEBUG:
            print(data)
    for i in range(0, len(data)):
        if '#define' in data[i]:
            if DEBUG:
                print(data[i])
            data[i] = RADIXJOINEX.format(0)
            if DEBUG:
                print(data[i])
    if DEBUG:
        print(data)
    with open('../src/include/duckdb/planner/operator/logical_comparison_join.hpp', 'w') as file:
        for e in data:
            file.write(e)
    file.close()
    
    

modifyFileDuckDBRadixExOn()
os.chdir("./RadixJoin")
os.system("python3 makePlotsRadix.py")
os.chdir("./..")

modifyFileDuckDBRadixExOff()
os.chdir("./HashJoin")
os.system("python3 makePlotsHash.py")
os.chdir("./..")

#for i in range(START,END):
#    modifyFileDuckDB(i)
#    # Change dir to make the new executable
#    os.chdir("../../build/release/benchmark")
#    # Configure and make the new executable
#    os.system("make -j8")
#    # Change back to the Desktop
#    os.chdir("../../../Benchmarks/RadixJoin")
#    # Wait to cool down
#    time.sleep(5) # sleep 5 seconds
#    # Execute the benchmarkrunner
#    os.system("python3 duckdbbenchmarkTimer.py")
#    # Wait to cool down
#    time.sleep(5) # sleep 5 seconds
#    # Change dir to make the new executable
#    os.chdir("../../build/release/benchmark")
#    # Configure and make the new executable
#    os.system("make clean")
#    # Change back to the Desktop
#    os.chdir("../../../Benchmarks/RadixJoin")
#
#
#time.sleep(10)
#
#pathDataRuntime = pathStart + b'/data_runtimeNoTimer.csv'
#fDataRuntime = open(pathDataRuntime, 'a+')
#for i in range(0, len(HEADINGSNOTIMER)):
#    fDataRuntime.write(HEADINGSNOTIMER[i])
#    if i != len(HEADINGSNOTIMER)-1:
#        fDataRuntime.write(",")
#    else:
#        fDataRuntime.write("\n")
#fDataRuntime.close()
#
#modifyFileDuckDBNoTimer()
#
#for i in range(START,END):
#    modifyFileDuckDB(i)
#    # Change dir to make the new executable
#    os.chdir("../../build/release/benchmark")
#    # Configure and make the new executable
#    os.system("make -j8")
#    # Change back to the Desktop
#    os.chdir("../../../Benchmarks/RadixJoin")
#    # Wait to cool down
#    time.sleep(5) # sleep 5 seconds
#    # Execute the benchmarkrunner
#    os.system("python3 duckdbbenchmarkNoTimer.py")
#    # Wait to cool down
#    time.sleep(5) # sleep 5 seconds
#    # Change dir to make the new executable
#    os.chdir("../../build/release/benchmark")
#    # Configure and make the new executable
#    os.system("make clean")
#    # Change back to the Desktop
#    os.chdir("../../../Benchmarks/RadixJoin")
