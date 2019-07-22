from kafka import KafkaProducer
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from keras.models import load_model
import numpy as np
import cv2
import imutils
from keras.applications.mobilenet import preprocess_input
import tensorflow as tf
import time

conf = SparkConf().setAppName("distract streaming").setMaster("yarn")
conf.set("spark.scheduler.mode", "FAIR")
conf.set("spark.scheduler.allocation.file", "/opt/spark-2.4.3-bin-hadoop2.7/conf/fairscheduler.xml")
sc = SparkContext(conf=conf)
sc.setLocalProperty("spark.scheduler.pool", "pool4")
ssc = StreamingContext(sc, 0.5)
sql_sc = SQLContext(sc)
input_topic = 'input'
output_topic = 'output4'
brokers = "G01-01:2181,G01-02:2181,G01-03:2181,G01-04:2181,G01-05:2181,G01-06:2181,G01-07:2181,G01-08:2181," \
          "G01-09:2181,G01-10:2181,G01-11:2181,G01-12:2181,G01-13:2181,G01-14:2181,G01-15:2181,G01-16:2181"

model_path = '/home/hduser/Distracted_mobilenet_full_6c.h5'  # /home/hduser/Distracted_vgg16_full.h5
model = load_model(model_path)
# graph = tf.get_default_graph()
# print(model.summary())
broadcast_model = sc.broadcast(model)


def distraction_detect(ss):
    key = ss[0]
    value = ss[1]
    image = np.asarray(bytearray(value), dtype="uint8")
    image_in = cv2.imdecode(image, cv2.IMREAD_ANYCOLOR)
    image = cv2.resize(image_in, (224, 224), interpolation=cv2.INTER_CUBIC)
    image = image.reshape((-1, 224, 224, 3))
    image = preprocess_input(image)
    graph = tf.get_default_graph()
    with graph.as_default():
        ynew = broadcast_model.value.predict_classes(image)
        result_dic = {0: "normal driving", 1: "texting", 2: "talking on the phone", 3: "operating on the radio",
                      4: "drinking", 5: "reaching behind"}
        image_in = imutils.resize(image_in, width=600)
        cv2.putText(image_in, "status: " + result_dic[ynew[0]], (10, 30),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
    return tuple([key, image_in])


def my_decoder(s):
    return s


kafkaStream = KafkaUtils.createStream(ssc, brokers, 'test-consumer-group-4', {input_topic: 15},
                                      valueDecoder=my_decoder)
producer = KafkaProducer(bootstrap_servers='G01-01:9092', compression_type='gzip', batch_size=163840,
                         buffer_memory=33554432, max_request_size=20485760)


def handler(message):
    newrdd = message.map(distraction_detect)
    for i in newrdd.collect():
        key = i[0]
        image_in = i[1]
        current = int(time.time() * 1000)
        if current - int(key) < 4500:
            producer.send(output_topic, value=cv2.imencode('.jpg', image_in)[1].tobytes(), key=key.encode('utf-8'))
            producer.flush()


kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination()
