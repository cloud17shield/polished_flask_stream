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
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

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


kafkaStream = KafkaUtils.createStream(ssc, brokers, 'test-consumer-group-2', {input_topic: 15},
                                      valueDecoder=my_decoder)
producer = KafkaProducer(bootstrap_servers='G01-01:9092', compression_type='gzip', batch_size=163840,
                         buffer_memory=33554432, max_request_size=20485760)

csv_file_path = "file:///home/hduser/polished_flask_stream/train_data48.csv"
predictor_path = "/home/hduser/shape_predictor_68_face_landmarks.dat"
model_path = "/home/hduser/polished_flask_stream/svc48.pickle"

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


def handler(message):
    records = message.collect()
    for record in records:
        try:
            print('record', len(record), type(record))
            print('-----------')
            print('tuple', type(record[0]), type(record[1]))
        except Exception:
            print("error")

        key = record[0]
        value = record[1]

        print("len", len(key), len(value))

        print("start processing")
        image = np.asarray(bytearray(value), dtype="uint8")
        # image = np.frombuffer(value, dtype=np.uint8)
        # img = image.reshape(300, 400, 3)
        # img = cv2.imread("/tmp/" + key)
        img = cv2.imdecode(image, cv2.IMREAD_ANYCOLOR)
        print('img shape', img, img.shape)
        frame = imutils.resize(img, width=600)
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        faces = detector(gray, 0)
        if len(faces) >= 1:
            predict_value = 0

            for face in faces:
                dic = {}
                x_values = [[] for _ in range(48)]
                y_values = [[] for _ in range(48)]
                (x, y, w, h) = rect_to_bb(face)
                # faceOrig = imutils.resize(img[y: y + h, x: x + w], width=100)
                faceAligned = fa.align(frame, gray, face)

                dets = detector(faceAligned, 0)
                num_face = len(dets)
                print(num_face)
                if num_face == 1:
                    for k, d in enumerate(dets):
                        shape = predictor(faceAligned, d)
                        for j in range(48):
                            x_values[j].append(shape.part(j).x)
                            y_values[j].append(shape.part(j).y)
                    for i in range(48):
                        dic['x' + str(i + 1)] = x_values[i]
                        dic['y' + str(i + 1)] = y_values[i]

                    df_score = pd.DataFrame(data=dic)
                    df_score = df_score[['x' + str(i) for i in range(1, 49)] + ['y' + str(j) for j in range(1, 49)]]
                    X_score = scaler.transform(df_score)
                    if True in clf2.predict(X_score):
                        predict_value = 1
                        break
            current = int(time.time() * 1000)
            if current - int(key) < 2000:
                cv2.putText(frame, "Drunk: " + str(predict_value), (10, 30),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
                print("drunk prediction:", predict_value)
                print("predict over")
                producer.send(output_topic, value=cv2.imencode('.jpg', frame)[1].tobytes(), key=key.encode('utf-8'))
                producer.flush()
                print('send over!')

        else:
            current = int(time.time() * 1000)
            if current - int(key) < 3000:
                cv2.putText(frame, "No face detected", (10, 30),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
                producer.send(output_topic, value=cv2.imencode('.jpg', frame)[1].tobytes(), key=key.encode('utf-8'))
                producer.flush()
                print('send over!')


kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination()
