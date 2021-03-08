import kafka
from time import time, sleep
from os import environ

try:
    server = environ['KAFKA_SERVER']
    if len(server) < 7:
        raise Exception
except:
    print('Variable KAFKA_SERVER is not set', flush=True)
    exit()


def create_topic(server, topic_name = 'input'):
    kafka_connector = kafka.admin.KafkaAdminClient( bootstrap_servers= server)
    kafka_topic = kafka.admin.NewTopic(
        name=topic_name,
        num_partitions=1,
        replication_factor=1
    )
    kafka_connector.create_topics(new_topics=[kafka_topic], validate_only=False)


def check_the_topic(server, topic_name = 'input'):
    kafka_connector = kafka.KafkaConsumer(bootstrap_servers = server)
    set_of_topics = kafka_connector.topics()
    if topic_name not in set_of_topics:
        create_topic(server)
        print('Topic "{}" has been created.' .format(topic_name), flush=True)
    return True


def write_to_topic(server, topic_name = 'input'):
    epoch_time = str(time())
    producer = kafka.KafkaProducer(bootstrap_servers=server)
    producer.send(topic_name, key = b'epoch', value = epoch_time.encode())
    return epoch_time


while True:
    try:
        if check_the_topic(server) == True:
            for i in range(100):
                print(write_to_topic(server), flush=True)
                sleep(10)
    except Exception as err:
        print(err, flush=True)
        sleep(60)
