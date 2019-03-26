#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest # construction des données
from pyspark import SparkContext

if __name__ == "__main__":
	# Creation d un contexte Spark
	sc=SparkContext(appName="Parallelize")

	data = [LabeledPoint(0.0, [0.0]), 
			LabeledPoint(0.0, [1.0]),
			LabeledPoint(1.0, [2.0]),
			LabeledPoint(1.0, [3.0])
			]
	# distribution de la table
	trainingData=sc.parallelize(data)
	trainingData.collect()
	# Estimation du modèle
	model = RandomForest.trainClassifier(trainingData, 2, {}, 3, seed=42)
	model.numTrees()
	model.totalNumNodes()

	# "Affichage" de la forêt
	print model.toDebugString()
	# Prévision d’u échantillon
	rdd = sc.parallelize([[3.0], [1.0]]) 
	model.predict(rdd).collect()

	sc.stop()