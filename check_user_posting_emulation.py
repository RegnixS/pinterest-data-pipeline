# %%
"""Check User Posting Emulation Module

This module allows some tests to check if the user_posting_emulation.py module works.
"""
import json
import requests
from time import sleep

def create_a_consumer(consumer_name):
    """
    Creates a Kafka consumer by posting to the API Gateway. 

    Args:
        String : The name of the consumer to create.
    """
    # Consumer Group has to be "students" as that's the only one authourized
    invoke_url = "https://mlbaqhr3m2.execute-api.us-east-1.amazonaws.com/test/consumers/students"
    
    # Start consumer from beginning
    payload = json.dumps({
        "name": consumer_name, "format": "json", "auto.offset.reset": "earliest"
    })
    
    headers = {'Content-Type': 'application/vnd.kafka.v2+json'}
    response = requests.request("POST", invoke_url, headers=headers, data=payload)
    
    print(response)
    print(response.json())

def subcribe_to_topic(consumer_name, topic_name):
    """
    Tells a Kafka consumer to subscribe to a topic. 

    Args:
        String : The name of the consumer that wants to subscribe.
        String : The name of the topic to subscribe to.
    """
    invoke_url = "https://mlbaqhr3m2.execute-api.us-east-1.amazonaws.com/test/consumers/students/instances/" + consumer_name + "/subscription"
    
    # subscribe to topic
    payload = json.dumps({
        "topics":[f"{topic_name}"]
    })
    
    headers = {'Content-Type': 'application/vnd.kafka.v2+json'}
    response = requests.request("POST", invoke_url, headers=headers, data=payload)
    
    print(response)

def consume_messages(consumer_name):
    """
    Consumes messages from Kafka. 

    Args:
        String : The name of the consumer.
    """
    invoke_url = "https://mlbaqhr3m2.execute-api.us-east-1.amazonaws.com/test/consumers/students/instances/" + consumer_name + "/records"
    
    headers = {'Accept': 'application/vnd.kafka.json.v2+json'}
    response = requests.request("GET", invoke_url, headers=headers)
    # Need to do it twice due to bug in Confluent REST Proxy
    sleep(10)
    response = requests.request("GET", invoke_url, headers=headers)

    print(response)
    print(response.json())
    
    # Ouput to a file
    with open(f'{consumer_name}.json', mode='a') as f:
        json.dump(response.json(), f)

def delete_consumer(consumer_name):
    """
    Delete the consumer to tidy up. 

    Args:
        String : The name of the consumer to delete.
    """
    invoke_url = "https://mlbaqhr3m2.execute-api.us-east-1.amazonaws.com/test/consumers/students/instances/" + consumer_name
    
    headers = {'Content-Type': 'application/vnd.kafka.v2+json'}
    response = requests.request("DELETE", invoke_url, headers=headers)
    
    print(response)

if __name__ == "__main__":
    """
    Check the emulation by creating consumers, subscribing to Kafka topics and consuming messages from those topics.
    Afterwards, delete the consumers to tidy up.
    """
    create_a_consumer('pin_consumer')
    subcribe_to_topic('pin_consumer', '129a67850695.pin')
    consume_messages('pin_consumer')
    delete_consumer('pin_consumer')

    create_a_consumer('geo_consumer')
    subcribe_to_topic('geo_consumer', '129a67850695.geo')
    consume_messages('geo_consumer')
    delete_consumer('geo_consumer')

    create_a_consumer('user_consumer')
    subcribe_to_topic('user_consumer', '129a67850695.user')
    consume_messages('user_consumer')
    delete_consumer('user_consumer')

    print('done')

# %%
