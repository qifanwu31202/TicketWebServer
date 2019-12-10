from kafka import KafkaConsumer
import socket
import sys
import requests
import requests_oauthlib
import json
import time

# consumer = KafkaConsumer('category')
# for msg in consumer:
#     val = (msg.value).decode('utf8').strip()
#     print(len(val))
#     print(type(val))
#     print(val)
#     print("------------------")

def kafka_to_spark(tcp_conn):
    consumer = KafkaConsumer('category')
    for msg in consumer:
        try:
            pure_text = (msg.value).decode('utf8').strip()  + '\n'
            # print(type(pure_text))
            print("pure text is : " + pure_text)
            time.sleep(0.5)
            tcp_conn.send(pure_text.encode('utf-8', errors='ignore'))
        except:
            e = sys.exc_info()[0]
            print("Error is: %s" % e)

    # for line in resp.iter_lines():
    #     try:
    #         all_tweet = json.loads(line)
    #         tweet_pure_text = all_tweet['text'] + '\n'      # pyspark can't accept stream, add '\n'
    #
    #         print("Tweet pure text is : " + tweet_pure_text)
    #         print ("------------------------------------------")
    #         tcp_conn.send(tweet_pure_text.encode('utf-8', errors='ignore'))
    #     except:
    #         e = sys.exc_info()[0]
    #         print("Error is: %s" % e)

# consumer = KafkaConsumer('category')
# for msg in consumer:
#     print((msg.value).decode('utf8'))


conn = None
TCP_IP = "localhost"
TCP_PORT = 9090
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((TCP_IP, TCP_PORT))
sock.listen(1)
print("Kafka end server is waiting for TCP connection...")
conn, addr = sock.accept()
print("Spark process connected. streaming to Spark process.")
kafka_to_spark(conn)