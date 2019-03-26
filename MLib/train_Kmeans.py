#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from numpy import array
from math import sqrt
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel

if __name__ == "__main__":
	# Creation d un contexte Spark
	sc=SparkContext(appName="fire train kmeans")

	# Lire un fichier csv et filtrer la première ligne
	data = sc.textFile('file:///home/sderrode/tp-hadoop-python/MLib/Data/modis_fire_2018_365_conus.csv')
	print(data.count())
	print(data.take(5))

	sc.stop()