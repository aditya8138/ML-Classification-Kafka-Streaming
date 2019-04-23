from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF,  Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName('pipeline').getOrCreate()
df = spark.read.format('csv').option("header", "true").option("mode", "DROPMALFORMED").load('input.csv')
df = df.select(df.label.cast('int').alias('label'), df.text.cast('string').alias('text'))

tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol= tokenizer.getOutputCol(), outputCol="filtered")
hashingTF = HashingTF(inputCol=remover.getOutputCol(), outputCol="features", numFeatures=10000)
pipeline = Pipeline(stages=[tokenizer,remover,hashingTF])
pip = pipeline.fit(df)
dataset = pip.transform(df)
dataset.show(5)

pip.write().overwrite().save('pipeline')

training, test = dataset.randomSplit([0.7, 0.3])

lr = LogisticRegression(maxIter=1000, regParam=0.03, elasticNetParam=0, family="multinomial")
lr_Model = lr.fit(training)
pred = lr_Model.transform(test)
pred.show(5)


lr_evaluator = MulticlassClassificationEvaluator(labelCol = 'label', predictionCol="prediction", metricName="accuracy")
lr_accuracy = lr_evaluator.evaluate(pred)
print("Accuracy of Logistic Regression")
print(lr_accuracy)
lr_Model.write().overwrite().save('lrModel')

nb = NaiveBayes(smoothing=1, modelType="multinomial")
nb_Model = nb.fit(training)
nb_pred = nb_Model.transform(test)

nb_evaluator = MulticlassClassificationEvaluator(labelCol = 'label', predictionCol="prediction", metricName="accuracy")
nb_accuracy = nb_evaluator.evaluate(nb_pred)
print("Accuracy of Naive Bayes")
print(nb_accuracy)
nb_Model.write().overwrite().save('nbModel')



# dataset.cache()
# idf = IDF(inputCol="hashed", outputCol="features")
# # lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
# rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
# pipeline2 = Pipeline(stages=[idf])
# pip2 = pipeline2.fit(dataset)
# finalDF = pip2.transform(dataset)
# finalDF.show(5)
