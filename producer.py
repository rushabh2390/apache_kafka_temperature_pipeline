from quixstreams import Application
from datetime import datetime,timedelta, timezone
import requests
import time
import json
url = "https://archive-api.open-meteo.com/v1/archive?latitude=23.0258&longitude=72.5873&start_date={}&end_date={}&hourly=temperature_2m"

# Create an Application - the main configuration entry point
app = Application(broker_address="localhost:9092", consumer_group="temperature_data")

# Define a topic with chat messages in JSON format
messages_topic = app.topic(name="messages", value_serializer="json")




def main():
    with app.get_producer() as producer:
        end_date = datetime.now().astimezone(timezone.utc)
        print(end_date)
        start_date = datetime(end_date.year,1,1)
        while start_date.date() != end_date.date():
            time.sleep(1)
            get_date = start_date.strftime("%Y-%m-%d")
            get_temperature_url = url.format(get_date,get_date)
            print(get_date)
            start_date = start_date + timedelta(days=1)
            payload = {}
            headers = {}
            response = requests.request("GET", get_temperature_url, headers=headers, data=payload)
            print(response.text, response.status_code)
            if response.status_code == 200:
                data = json.loads(response.text)
                
                kafka_msg = messages_topic.serialize(key=str(start_date.date()), value=data)

        # Produce chat message to the topic
                print(f'Produce event with key="{kafka_msg.key}" value="{kafka_msg.value}"')
                producer.produce(
                    topic=messages_topic.name,
                    key=kafka_msg.key,
                    value=kafka_msg.value,
                )


if __name__ == "__main__":
    main()