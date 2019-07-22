from kafka import KafkaProducer
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pickle
from imutils.face_utils import FaceAligner
from imutils.face_utils import rect_to_bb
import numpy as np
import imutils
import dlib
import cv2
import time
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from io import BytesIO
from PIL import Image

conf = SparkConf().setAppName("drunk video stream").setMaster("yarn")
conf.set("spark.scheduler.mode", "FAIR")
conf.set("spark.scheduler.allocation.file", "/opt/spark-2.4.3-bin-hadoop2.7/conf/fairscheduler.xml")
sc = SparkContext(conf=conf)
sc.setLocalProperty("spark.scheduler.pool", "pool2")
ssc = StreamingContext(sc, 0.5)
sql_sc = SQLContext(sc)
input_topic = 'input'
output_topic = 'output2'
brokers = "G01-01:2181,G01-02:2181,G01-03:2181,G01-04:2181,G01-05:2181,G01-06:2181,G01-07:2181,G01-08:2181," \
          "G01-09:2181,G01-10:2181,G01-11:2181,G01-12:2181,G01-13:2181,G01-14:2181,G01-15:2181,G01-16:2181"


def my_decoder(s):
    return s


# numStreams = 5
# kafkaStreams = [KafkaUtils.createStream(ssc, brokers, 'test-consumer-group', {input_topic: 10}, valueDecoder=my_decoder) for _ in range (numStreams)]
# unifiedStream = ssc.union(*kafkaStreams)
kafkaStream = KafkaUtils.createStream(ssc, brokers, 'test-consumer-group-2', {input_topic: 15},
                                      valueDecoder=my_decoder)
producer = KafkaProducer(bootstrap_servers='G01-01:9092', compression_type='gzip', batch_size=163840,
                         buffer_memory=33554432, max_request_size=20485760)

csv_file_path = "file:///home/hduser/DrunkDetection/train_data48-100.csv"
predictor_path = "/home/hduser/DrunkDetection/shape_predictor_68_face_landmarks.dat"
model_path = "/home/hduser/DrunkDetection/rf48-100.pickle"

df = pd.read_csv(csv_file_path, index_col=0)
print(df.columns)
df_y = df['label'] == 3
df_X = df[['x' + str(i) for i in range(1, 49)] + ['y' + str(j) for j in range(1, 49)]]
X_train, X_test, y_train, y_test = train_test_split(df_X, df_y, test_size=0.2, random_state=15)
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
detector = dlib.get_frontal_face_detector()
predictor = dlib.shape_predictor(predictor_path)
fa = FaceAligner(predictor, desiredFaceWidth=100)
with open(model_path, 'rb') as f:
    clf2 = pickle.load(f)

broadcast_detector = sc.broadcast(detector)
broadcast_predictor = sc.broadcast(predictor)
broadcast_fa = sc.broadcast(fa)
broadcast_clf2 = sc.broadcast(clf2)
broadcast_scaler = sc.broadcast(scaler)


def drunk_detect(ss):
    key = ss[0]
    value = ss[1]

    # producer = KafkaProducer(bootstrap_servers='G01-01:9092', compression_type='gzip', batch_size=163840,
    #                          buffer_memory=33554432, max_request_size=20485760)


    image = np.asarray(bytearray(value), dtype="uint8")
    img = cv2.imdecode(image, cv2.IMREAD_ANYCOLOR)
    print('img shape', img, img.shape)
    frame = imutils.resize(img, width=600)
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    faces = broadcast_detector.value(gray, 0)
    predict_value = 0

    if len(faces) >= 1:



        for face in faces:

            dic = {}
            x_values = [[] for _ in range(48)]
            y_values = [[] for _ in range(48)]
            (x, y, w, h) = rect_to_bb(face)
            # faceOrig = imutils.resize(img[y: y + h, x: x + w], width=100)
            faceAligned = broadcast_fa.value.align(frame, gray, face)

            dets = broadcast_detector.value(faceAligned, 0)
            num_face = len(dets)
            print(num_face)
            if num_face == 1:
                for k, d in enumerate(dets):
                    shape = broadcast_predictor.value(faceAligned, d)
                    for j in range(48):
                        x_values[j].append(shape.part(j).x)
                        y_values[j].append(shape.part(j).y)
                for i in range(48):
                    dic['x' + str(i + 1)] = x_values[i]
                    dic['y' + str(i + 1)] = y_values[i]

                df_score = pd.DataFrame(data=dic)
                df_score = df_score[['x' + str(i) for i in range(1, 49)] + ['y' + str(j) for j in range(1, 49)]]
                X_score = broadcast_scaler.value.transform(df_score)
                if True in broadcast_clf2.value.predict(X_score):
                    predict_value = 1
                    break
        cv2.putText(frame, "Drunk: " + str(predict_value), (10, 30),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
    #     producer.send("output2", value=cv2.imencode('.jpg', frame)[1].tobytes(), key=key.encode('utf-8'))
    #     producer.flush()
    else:
        cv2.putText(frame, "No face detected", (10, 30),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
    #     producer.send("output2", value=cv2.imencode('.jpg', frame)[1].tobytes(), key=key.encode('utf-8'))
    #     producer.flush()

    return tuple([key, frame])


def handler(message):
    newrdd = message.map(drunk_detect)
    for i in newrdd.collect():
        # print("text23333?", i)
        # print("return type:", type(i))
        key = i[0]
        frame = i[1]
        current = int(time.time() * 1000)
        if current - int(key) < 3000:
            producer.send("output2", value=cv2.imencode('.jpg', frame)[1].tobytes(), key=key.encode('utf-8'))
            producer.flush()


kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination()
