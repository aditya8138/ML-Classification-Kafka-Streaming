from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pandas as pd
from kafka import KafkaConsumer
import sys
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel, NaiveBayesModel
from sklearn.metrics import accuracy_score, recall_score,precision_score

sc = SparkContext()
sqlContext = SQLContext(sc)

spark = SparkSession.builder.appName('consumer').getOrCreate()
brokers , topic  = sys.argv[1:]
consumer = KafkaConsumer(topic, bootstrap_servers = ['localhost:9092'])

pip = PipelineModel.load('/Users/aditya/PycharmProjects/BigDataHW3/pipeline')
model_nb = NaiveBayesModel.load('/Users/aditya/PycharmProjects/BigDataHW3/nbModel')
model_lr = LogisticRegressionModel.load('/Users/aditya/PycharmProjects/BigDataHW3/lrModel')

columns = ['actual','predicted']
result_df_lr = pd.DataFrame(columns = columns)
result_df_nb = pd.DataFrame(columns = columns)
feed = 0

for msg in consumer:
    article = msg.value
    data = article.split("||")
    label = data[0]
    text = data[1]
    df = sc.parallelize([{"label": label, "text": text}]).toDF()
    df = pip.transform(df)
    nb_pred = model_nb.transform(df)
    lr_pred = model_lr.transform(df)

    lr_el = lr_pred.select('prediction').collect()[0]['prediction']
    nb_el = nb_pred.select('prediction').collect()[0]['prediction']

    result_df_lr = result_df_lr.append({'actual': float(label), 'predicted':lr_el}, ignore_index=True)
    result_df_nb = result_df_nb.append({'actual': float(label), 'predicted':nb_el}, ignore_index=True)
    feed = feed + 1
    print(feed)
    if (feed == 30):
        feed = 0
        accuracy_lr = accuracy_score(result_df_lr['actual'], result_df_lr['predicted'])
        recall_lr =recall_score(result_df_lr['actual'], result_df_lr['predicted'], average= 'weighted')
        precision_lr = precision_score(result_df_lr['actual'], result_df_lr['predicted'], average= 'weighted')
        print("***********************************************************************************************************")
        print("Logistic Regression Accuracy "+str(accuracy_lr))
        print("Logistic Regression Recall "+str(recall_lr))
        print("Logistic Regression Precision "+str(precision_lr))
        print(
            "***********************************************************************************************************")
        accuracy_nb = accuracy_score(result_df_nb['actual'], result_df_nb['predicted'])
        recall_nb = recall_score(result_df_nb['actual'], result_df_nb['predicted'], average='weighted')
        precision_nb = precision_score(result_df_nb['actual'], result_df_nb['predicted'], average='weighted')
        print("Naive Bayes Accuracy "+str(accuracy_nb))
        print("Naive Bayes Recall "+str(recall_nb))
        print("Naive Bayes Precision "+str(precision_nb))
        result_df_lr = pd.DataFrame(columns=columns)
        result_df_nb = pd.DataFrame(columns=columns)
