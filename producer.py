from kafka import KafkaProducer

#  connect to Kafka

input_topic = 'testinput'
output_topic = 'testoutput'
brokers = "G01-01:2181,G01-02:2181,G01-03:2181,G01-04:2181,G01-05:2181,G01-06:2181,G01-07:2181,G01-08:2181," \
          "G01-09:2181,G01-10:2181,G01-11:2181,G01-12:2181,G01-13:2181,G01-14:2181,G01-15:2181,G01-16:2181 "
producer = KafkaProducer(bootstrap_servers='G01-01:9092',compression_type='gzip',batch_size=163840,buffer_memory=33554432,max_request_size=20485760)
# Assign a topic

# Hipsum paragraph as an example set of text data to stream
message_data = "Lorem ipsum dolor amet lo-fi venmo knausgaard, roof party activated charcoal franzen prism mlkshk subway tile williamsburg jean shorts waistcoat cold-pressed. Sriracha microdosing pug squid deep v, you probably haven't heard of them swag try-hard pok pok snackwave quinoa actually shaman selfies hashtag. Twee tofu copper mug, kickstarter butcher cred glossier. Selfies bespoke woke kale chips kickstarter biodiesel edison bulb vice normcore, irony fam. Subway tile marfa asymmetrical squid yr meh next level. Kafka Stream data complete. <hr><br>".split(" ")

def message_emitter(message_data):
    print("Beginning Message Kafka Stream")
    msg_num = 1
    for msg in message_data:
        # encode the key and value as bytes before sending
        producer.send(input_topic, 
            key=bytes(str(msg_num), 'utf-8'),  
            value=bytes(msg, 'utf-8')).get(timeout=60)
        
        msg_num +=1
        
    print("Emit complete")


if __name__ == "__main__":
	message_emitter(message_data)
