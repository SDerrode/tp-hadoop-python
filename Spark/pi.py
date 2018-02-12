import sys

from operator import add
from random import random

from pyspark import SparkContext

def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 < 1 else 0

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    sc = SparkContext(appName="PythonPi")
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    count = sc.parallelize(range(n), numSlices=partitions).map(f).reduce(add)
    print ("Pi is roughly ", 4.0 *  count / n)

    sc.stop()
