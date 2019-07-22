from kafka import KafkaProducer
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import numpy as np
import imutils
import cv2
from imageai.Detection import ObjectDetection
import tensorflow as tf
import time

conf = SparkConf().setAppName("object detection streaming").setMaster("yarn")
conf.set("spark.scheduler.mode", "FAIR")
conf.set("spark.scheduler.allocation.file", "/opt/spark-2.4.3-bin-hadoop2.7/conf/fairscheduler.xml")
sc = SparkContext(conf=conf)
sc.setLocalProperty("spark.scheduler.pool", "pool3")
ssc = StreamingContext(sc, 0.5)
sql_sc = SQLContext(sc)
input_topic = 'input'
output_topic = 'output3'
brokers = "G01-01:2181,G01-02:2181,G01-03:2181,G01-04:2181,G01-05:2181,G01-06:2181,G01-07:2181,G01-08:2181," \
          "G01-09:2181,G01-10:2181,G01-11:2181,G01-12:2181,G01-13:2181,G01-14:2181,G01-15:2181,G01-16:2181"


def my_decoder(s):
    return s


kafkaStream = KafkaUtils.createStream(ssc, brokers, 'test-consumer-group-3', {input_topic: 15},
                                      valueDecoder=my_decoder)
producer = KafkaProducer(bootstrap_servers='G01-01:9092', compression_type='gzip', batch_size=163840,
                         buffer_memory=33554432, max_request_size=20485760)

detector = ObjectDetection()
detector.setModelTypeAsTinyYOLOv3()  # !!!tiny
detector.setModelPath('/home/hduser/yolo-tiny.h5')
detector.loadModel(detection_speed="flash")
custom = detector.CustomObjects(person=True, bottle=True, knife=True, cell_phone=True, fork=True)
graph = tf.get_default_graph()


def handler(message):
    records = message.collect()
    for record in records:
        try:
            print('record', len(record), type(record))
            print('-----------')
            print('tuple', type(record[0]), type(record[1]))
        except Exception:
            print("error")
        # producer.send(output_topic, b'message received')
        key = record[0]
        value = record[1]
        print("len", len(key), len(value))

        print("start processing")
        global graph
        global model
        global custom
        with graph.as_default():
            image = np.asarray(bytearray(value), dtype="uint8")
            # image = np.frombuffer(value, dtype=np.uint8)
            # img = image.reshape(300, 400, 3)
            # img = cv2.imread("/tmp/" + key)
            img = cv2.imdecode(image, cv2.IMREAD_ANYCOLOR)
            frame = imutils.resize(img, width=600)
            # img_array = np.array(frame)
            detected_image_array, detections = detector.detectCustomObjectsFromImage(custom_objects=custom,
                                                                                     input_type="array",
                                                                                     input_image=frame,
                                                                                     output_type="array")
            # image_really = Image.fromarray(detected_image_array.astype('uint8')).convert('RGB')
            current = int(time.time() * 1000)
            if current - int(key) < 3000:
                producer.send(output_topic, value=cv2.imencode('.jpg', detected_image_array)[1].tobytes(),
                              key=key.encode('utf-8'))
                producer.flush()
                print('send over!')


kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination()
