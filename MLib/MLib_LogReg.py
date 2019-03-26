#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark import SparkContext


if __name__ == "__main__":
	# Creation d un contexte Spark
	sc=SparkContext(appName="Parallelize")

	spam = sc.textFile("spam.txt")
	normal = sc.textFile("ham.txt")

	tf             = HashingTF(numFeatures = 10000)
	spamFeatures   = spam.map  (lambda email:tf.transform(email.split(' ')))
	normalFeatures = normal.map(lambda email:tf.transform(email.split(' ')))

	posExamples = spamFeatures.map  (lambda features: LabeledPoint(1, features))
	negExamples = normalFeatures.map(lambda features: LabeledPoint(0, features))
	
	trainingData = posExamples.union(negExamples)
	trainingData.cache()

	model=LogisticRegressionWithSGD.train(trainingData)
	posTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(' '))
	negTest = tf.transform("Hi Dad, I started studying Spark the other ...".split(' '))
	
	print('Prediction for positive test example: ', model.predict(posTest))
	print('Prediction for negative test example: ', model.predict(negTest))

	sc.stop()