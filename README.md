# FYP
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
Install gdown in linux server to download model from my [google drive][1]:
```bash
pip3 install --user gdown
```
Then download the model:
```bash
# Distracted_mobilenet_full.h5
gdown "https://drive.google.com/uc?id=1Na8QXTkO0J1OpMRs0u-AQ9A6jUvLo7DL"
# Distracted_mobilenet_full_6c.h5
gdown "https://drive.google.com/uc?id=1WlraW_Wvb-nIUk_AeXDGGGkNv_mjP1tg"

# Drunk and drowsy human face detect
gdown "https://drive.google.com/uc?id="
```

For [Imageai object][2] detection [pretrained model][3]:
```bash
# yolov3
wget https://github.com/OlafenwaMoses/ImageAI/releases/download/1.0/yolo.h5
# yolov3tiny
wget https://github.com/OlafenwaMoses/ImageAI/releases/download/1.0/yolo-tiny.h5
```

## How to 
Refer to [https://github.com/cloud17shield/k8s\_env][4]
Set spark configuration, download all the model and copy to every node, modify code: model path, ip and port, Kafka related.

Then run with spark streaming submit cluster mode.

[1]:	https://drive.google.com/drive/u/1/folders/1TbzGQnVUSgJeUlpILeXQ-CQsg473beE5
[2]:	https://imageai.readthedocs.io/en/latest/video/index.html
[3]:	https://github.com/OlafenwaMoses/ImageAI/releases/tag/1.0/
[4]:	https://github.com/cloud17shield/k8s_env