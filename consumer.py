from flask import Flask, Response


app = Flask(__name__)


@app.route('/')
def index():
    # return a multipart response

    return Response(kafka_stream(), mimetype='multipart/x-mixed-replace; boundary=frame')


def kafka_stream():
    for msg in consumer:
        print('start playing...')
        print(len(msg), len(msg.value))
        print(type(msg.value))
        print('key:', msg.key)
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
            print(image_len)
            if not image_len:
                break

            image_stream = io.BytesIO()
            # 读取图片
            image_stream.write(connection.read(image_len))
            image_stream.seek(0)

            image = Image.open(image_stream)
            cv2img = numpy.array(image, dtype=numpy.uint8)[:, :, ::-1]

            # send image stream to kafka
            print('imgshape', cv2img.shape)
            producer.send(input_topic, value=cv2.imencode('.jpg', cv2img)[1].tobytes(),
                          key=str(int(time.time() * 1000)).encode('utf-8'))
            producer.flush()

    except Exception as e:
        print('error streaming client', str(e))


if __name__ == '__main__':
    app.run(host="10.244.27.7", debug=True, port=54321, threaded=True)
