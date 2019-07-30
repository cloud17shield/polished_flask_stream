# FYP

**Real-time Drivers’ Behavior Anomaly Detection**

Our project website [https://i.cs.hku.hk/\~msp18018/][1] 

---- 

## Code

### WEB

`consumer_4together_polished.py` python flask WEB UI, front end, render 4 detection output and keep the socket server connection online.
Run following command to start the web server:

```python
python3 consumer_4together_polished.py
```

`client.py` socket client, using in local client, using web camera to socket transmit the live video data(frames). Run it in you local machine(laptop).

### Spark Streaming

`d…*_streaming.py` first version code, using collect() methods to computing in driver, not scaleable. 

`d…*_streaming_distributed.py` second version code, using map() method to parallel compute, using broadcast() to share model and static variable in each node. Need warm up due to the broadcast, waiting for several minutes it will stable, (depends on the model size). Also with lower latency and higher FPS.
Run following(e.g. drowsy):

```bash
/opt/spark-2.4.3-bin-hadoop2.7/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 5 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.3 ~/DrunkDetection/drowsy_streaming_distributed.py
```

---- 

## Model Download

Install gdown in linux server to download model from my [google drive][2] (these model is larger \>100M):

```bash
pip3 install --user gdown
```

Then download the model using gdown:

```bash
# Distracted_mobilenet_full.h5
gdown "https://drive.google.com/uc?id=1Na8QXTkO0J1OpMRs0u-AQ9A6jUvLo7DL"

# Distracted_mobilenet_full_6c.h5
gdown "https://drive.google.com/uc?id=1WlraW_Wvb-nIUk_AeXDGGGkNv_mjP1tg"

# Drunk and drowsy human face landmarking
# shape_predictor_68_face_landmarks.dat
gdown "https://drive.google.com/uc?id=1s4Gzq8H_XNqgVqIyLZhOSbNVx-fbKrBg"
# the code have already contains pickle file and csv (small)
```

For [Imageai object][3] detection [pre-trained model][4]:

```bash
# yolov3
wget https://github.com/OlafenwaMoses/ImageAI/releases/download/1.0/yolo.h5

# yolov3tiny
wget https://github.com/OlafenwaMoses/ImageAI/releases/download/1.0/yolo-tiny.h5
```

## How to

Refer to [https://github.com/cloud17shield/k8s\_env][5]: [spark-default.conf][6], and refer to [k8s.sh][7], some parameters and steps there.

Set spark configuration, download all the model and copy to every node, modify code: model path, ip and port, Kafka related.

Then run with spark streaming submit cluster mode. Run the flask server and fresh, run client start the camera. You will see the result in the browser.

[1]:	https://i.cs.hku.hk/~msp18018/
[2]:	https://drive.google.com/drive/u/1/folders/1TbzGQnVUSgJeUlpILeXQ-CQsg473beE5
[3]:	https://imageai.readthedocs.io/en/latest/video/index.html
[4]:	https://github.com/OlafenwaMoses/ImageAI/releases/tag/1.0/
[5]:	https://github.com/cloud17shield/k8s_env
[6]:	https://github.com/cloud17shield/k8s_env/blob/master/spark-defaults.conf
[7]:	https://github.com/cloud17shield/k8s_env/blob/master/k8s.sh