from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

try:
    topic = sys.argv[1]
except:
    print('Usage: python3 consumer <input_topic> [output_topic]')
    exit(1)

output_topic = sys.argv[2] if len(sys.argv) > 2 else None

# Create consumer: Option 1 -- only consume new events
consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

# Create consumer: Option 2 -- consume old events (uncomment to test -- and comment Option 1 above)
#consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')

if output_topic:
    producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
    print(f'Bridging {topic} --> {output_topic}')

consumer.subscribe([topic])
for msg in consumer:
    print(msg.value)
    if output_topic:
        processed = b'[processed] ' + msg.value
        producer.send(output_topic, value=processed)
        producer.flush()
