from kafka import KafkaProducer
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from imutils import face_utils
from scipy.spatial import distance
import numpy as np
import imutils
import dlib
import cv2
import time

conf = SparkConf().setAppName("drowsy streaming v2").setMaster("yarn")
conf.set("spark.scheduler.mode", "FAIR")
sc = SparkContext(conf=conf)
sc.setLocalProperty("spark.scheduler.pool", "pool1")
ssc = StreamingContext(sc, 0.2)
sql_sc = SQLContext(sc)
input_topic = 'input'
output_topic = 'output1'
brokers = "G01-01:2181,G01-02:2181,G01-03:2181,G01-04:2181,G01-05:2181,G01-06:2181,G01-07:2181,G01-08:2181," \
          "G01-09:2181,G01-10:2181,G01-11:2181,G01-12:2181,G01-13:2181,G01-14:2181,G01-15:2181,G01-16:2181"
predictor_path = "/home/hduser/shape_predictor_68_face_landmarks.dat"


def my_decoder(s):
    return s


kafkaStream = KafkaUtils.createStream(ssc, brokers, 'test-consumer-group-1', {input_topic: 15},
                                      valueDecoder=my_decoder)
producer = KafkaProducer(bootstrap_servers='G01-01:9092', compression_type='gzip', batch_size=163840,
                         buffer_memory=33554432, max_request_size=20485760)
thresh = 0.25
frame_check = 20
detect = dlib.get_frontal_face_detector()
predict = dlib.shape_predictor(predictor_path)  # Dat file is the crux of the code

flag = 0

broadcast_detect = sc.broadcast(detect)
broadcast_predict = sc.broadcast(predict)


def drowsy_detect(ss):
    def eye_aspect_ratio(eye):
        A = distance.euclidean(eye[1], eye[5])
        B = distance.euclidean(eye[2], eye[4])
        C = distance.euclidean(eye[0], eye[3])
        ear = (A + B) / (2.0 * C)
        return ear

    key = ss[0]
    value = ss[1]
    ear = 1

    (lStart, lEnd) = face_utils.FACIAL_LANDMARKS_68_IDXS["left_eye"]
    (rStart, rEnd) = face_utils.FACIAL_LANDMARKS_68_IDXS["right_eye"]

    image = np.asarray(bytearray(value), dtype="uint8")
    img = cv2.imdecode(image, cv2.IMREAD_ANYCOLOR)
    frame = imutils.resize(img, width=600)
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    subjects = broadcast_detect.value(gray, 0)
    for subject in subjects:
        shape = broadcast_predict.value(gray, subject)
        shape = face_utils.shape_to_np(shape)
        leftEye = shape[lStart:lEnd]
        rightEye = shape[rStart:rEnd]
        leftEAR = eye_aspect_ratio(leftEye)
        rightEAR = eye_aspect_ratio(rightEye)
        ear = (leftEAR + rightEAR) / 2.0
        leftEyeHull = cv2.convexHull(leftEye)
        rightEyeHull = cv2.convexHull(rightEye)
        cv2.drawContours(frame, [leftEyeHull], -1, (0, 255, 0), 1)
        cv2.drawContours(frame, [rightEyeHull], -1, (0, 255, 0), 1)

    return tuple([key, frame, ear])


def handler(message):
    global flag
    newrdd = message.map(drowsy_detect)
    for i in newrdd.collect():
        key = i[0]
        frame = i[1]
        ear = i[2]
        print("return", key, frame, ear)
        if ear < thresh:
            flag += 1
            if flag >= frame_check:
                cv2.putText(frame, "********************DROWSY!********************", (10, 30),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
                cv2.putText(frame, "********************DROWSY!********************", (10, 400),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
        else:
            flag = 0
        current = int(time.time() * 1000)
        if current - int(key) < 3000:
            producer.send(output_topic, value=cv2.imencode('.jpg', frame)[1].tobytes(), key=key.encode('utf-8'))
            producer.flush()


kafkaStream.foreachRDD(handler)
ssc.start()
ssc.awaitTermination()
