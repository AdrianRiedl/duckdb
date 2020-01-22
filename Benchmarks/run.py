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



print("ChunkCollectionJoinHashTable")
os.chdir("..")
os.system("git checkout ChunkCollectionJoinHashTable")
os.chdir("./Benchmarks")

modifyFileDuckDBRadixExOn()
os.chdir("./RadixJoin")
os.system("python3 makePlotsRadix.py")
os.chdir("./..")

modifyFileDuckDBRadixExOff()
os.chdir("./HashJoin")
os.system("python3 makePlotsHash.py")
os.chdir("./..")

print("ChunkCollectionRadixJoin")
os.chdir("..")
os.system("git checkout ChunkCollectionRadixJoin")
os.chdir("./Benchmarks")

modifyFileDuckDBRadixExOn()
os.chdir("./RadixJoin")
os.system("python3 makePlotsRadix.py")
os.chdir("./..")

modifyFileDuckDBRadixExOff()
os.chdir("./HashJoin")
os.system("python3 makePlotsHash.py")
os.chdir("./..")

print("master")
os.chdir("..")
os.system("git checkout master")
os.chdir("./Benchmarks")

modifyFileDuckDBRadixExOn()
os.chdir("./RadixJoin")
os.system("python3 makePlotsRadix.py")
os.chdir("./..")

modifyFileDuckDBRadixExOff()
os.chdir("./HashJoin")
os.system("python3 makePlotsHash.py")
os.chdir("./..")
