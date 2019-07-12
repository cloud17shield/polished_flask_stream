from flask import Flask, render_template, send_from_directory, flash, request, redirect, url_for, g, Response
from kafka import KafkaConsumer
import socket
import struct
import io
from kafka import KafkaProducer
import time
from PIL import Image
import numpy
import cv2

input_topic = 'input'
output_topic = 'output'
# connect to Kafka server and pass the topic we want to consume
brokers = "G01-01:2181,G01-02:2181,G01-03:2181,G01-04:2181,G01-05:2181,G01-06:2181,G01-07:2181,G01-08:2181," \
          "G01-09:2181,G01-10:2181,G01-11:2181,G01-12:2181,G01-13:2181,G01-14:2181,G01-15:2181,G01-16:2181"

consumer1 = KafkaConsumer('output1', group_id='test-consumer-group-1', bootstrap_servers='g01-01:9092')
consumer2 = KafkaConsumer('output2', group_id='test-consumer-group-2', bootstrap_servers='g01-01:9092')
consumer3 = KafkaConsumer('output3', group_id='test-consumer-group-3', bootstrap_servers='g01-01:9092')
consumer4 = KafkaConsumer('output4', group_id='test-consumer-group-4', bootstrap_servers='g01-01:9092')

producer = KafkaProducer(bootstrap_servers='G01-01:9092', compression_type='gzip', batch_size=163840,
                         buffer_memory=33554432, max_request_size=20485760)

app = Flask(__name__)
app.config.from_object(__name__)


@app.route('/')
def index():
    return render_template('charts.html')
    # return a multipart response


@app.route('/drunk_detect')
def drunk_detect():
    return Response(kafka_stream2(), mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/drowsy_detect')
def drowsy_detect():
    return Response(kafka_stream1(), mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/dangerous_object_detect')
def dangerous_object_detect():
    return Response(kafka_stream3(), mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/distracted_detect')
def distracted_detect():
    return Response(kafka_stream4(), mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/socket_app')
def socket_app():
    return Response(socket_streaming(), mimetype='multipart/x-mixed-replace; boundary=frame')


def kafka_stream1():
    for msg in consumer1:
        print('Drowsy', msg.key, len(msg.value), msg.timestamp, int(msg.timestamp) - int(msg.key.decode('utf-8')))
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + msg.value + b'\r\n\r\n')


def kafka_stream2():
    for msg in consumer2:
        print('Drunk', msg.key, len(msg.value), msg.timestamp, int(msg.timestamp) - int(msg.key.decode('utf-8')))
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + msg.value + b'\r\n\r\n')


def kafka_stream3():
    for msg in consumer3:
        print('Danger', msg.key, len(msg.value), msg.timestamp, int(msg.timestamp) - int(msg.key.decode('utf-8')))
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + msg.value + b'\r\n\r\n')


def kafka_stream4():
    for msg in consumer4:
        print('Distract', msg.key, len(msg.value), msg.timestamp, int(msg.timestamp) - int(msg.key.decode('utf-8')))
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + msg.value + b'\r\n\r\n')


def socket_streaming():
    server_socket = socket.socket()
    # 绑定socket通信端口
    server_socket.bind(('10.244.27.7', 23333))
    server_socket.listen(0)
    print("socket establish")

    connection = server_socket.accept()[0].makefile('rb')
    print("connection establish")
    try:
        while True:
            # 获得图片长度
            image_len = struct.unpack('<L', connection.read(struct.calcsize('<L')))[0]
            print('socket', image_len)
            if not image_len:
                break

            image_stream = io.BytesIO()
            # 读取图片
            image_stream.write(connection.read(image_len))
            image_stream.seek(0)

            image = Image.open(image_stream)
            cv2img = numpy.array(image, dtype=numpy.uint8)[:, :, ::-1]

            # send image stream to kafka
            # print('imgshape', cv2img.shape)
            producer.send(input_topic, value=cv2.imencode('.jpg', cv2img)[1].tobytes(),
                          key=str(int(time.time() * 1000)).encode('utf-8'))
            producer.flush()

    except Exception as e:
        print('error streaming client', str(e))


if __name__ == '__main__':
    app.run(host="10.244.27.7", debug=True, port=54321, threaded=True)
