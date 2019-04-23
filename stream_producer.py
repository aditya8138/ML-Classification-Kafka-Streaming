# -*- coding: utf-8 -*-

from kafka import KafkaProducer
from time import sleep
import json
import sys
import requests
import time


def getData(url):
    jsonData = requests.get(url).json()
    data = []
    labels = {'Australia news': 0, 'US news': 1, 'Football': 2, 'World news': 3, 'Sport': 4, 'Television & radio': 5, 'Environment': 6, 'Science': 7, 'Media': 8, 'News': 9, 'Opinion': 10, 'Politics': 11, 'Business': 12, 'UK news': 13, 'Society': 14, 'Life and style': 15, 'Inequality': 16, 'Art and design': 17, 'Books': 18, 'Stage': 19, 'Film': 20, 'Music': 21, 'Global development': 22, 'Food': 23, 'Culture': 24, 'Community': 25, 'Money': 26, 'Technology': 27, 'Travel': 28, 'From the Observer': 29, 'Fashion': 30, 'Crosswords': 31, 'Law': 32, 'Education': 33, 'Membership': 34, 'Global': 35, 'Cities': 36, 'Love family life': 37, 'Games': 38, 'Info': 39, 'GNM press office': 40, 'Guardian Masterclasses': 41, "Coventry children's services": 42, 'Weather': 43, 'Entertainment One: On the Basis of Sex': 44, 'From the Guardian': 45, 'Roche personalised healthcare': 46, 'GNM education centre': 47, 'Connecting business': 48, 'Developing sustainable solutions – University of Plymouth': 49, 'Brother: At your side': 50, 'Guardian discount codes': 51, 'Breakthrough science': 52, 'Higher Education Network': 53, 'Discover your Wales': 54, 'Finance matters': 55, 'Cafcass professional careers': 56, 'Transforming the student experience': 57, 'Bank Australia: Clean money': 58, 'Green shoots': 59, 'Detectives transforming communities': 60, 'Modern money by HSBC UK': 61, 'Behind every great holiday': 62, 'PwC partner zone': 63, 'Global Challenges Foundation': 64, 'Guardian Careers': 65, 'Careers with Konica Minolta': 66}
    index = 0

    for i in range(len(jsonData["response"]['results'])):
        headline = jsonData["response"]['results'][i]['fields']['headline']
        bodyText = jsonData["response"]['results'][i]['fields']['bodyText']
        headline += bodyText
        label = jsonData["response"]['results'][i]['sectionName']
        # if label not in labels:
        #     labels[label] = index
        #     index += 1
        # data.append({'label':labels[label],'Descript':headline})
        toAdd = str(labels[label]) + '||' + headline
        data.append(toAdd)
    return(data)


def publish_message(producer_instance, topic_name, value):
    try:
        key_bytes = bytes('foo', encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        #_producer = KafkaProducer(value_serializer=lambda v:json.dumps(v).encode('utf-8'),bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), linger_ms=10)

    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


if __name__ == "__main__":

    # if len(sys.argv) != 4:
    #     print ('Number of arguments is not correct')
    #     exit()

    key = sys.argv[1]
    fromDate = sys.argv[2]
    toDate = sys.argv[3]

    url = 'http://content.guardianapis.com/search?from-date=' + fromDate + '&to-date=' + toDate + '&order-by=newest&show-fields=all&page-size=200&%20num_per_section=10000&api-key=' + key
    all_news = getData(url)
    if len(all_news) > 0:
        prod = connect_kafka_producer()
        for story in all_news:
            print(json.dumps(story))
            publish_message(prod, 'guardian2', story)
            time.sleep(1)
        if prod is not None:
            prod.close()
