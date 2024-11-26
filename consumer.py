from quixstreams import Application
import json
import time
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2 import sql
# Create an Application - the main configuration entry point
app = Application(
    broker_address="localhost:9092",
    consumer_group="temperature_data",
    auto_offset_reset="earliest",
)
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}
def insert_into_db(data):
    print(f"Input Data: {data}")
    temperature_data = {
        "date" : datetime.strptime(data["hourly"]["time"][0],"%Y-%m-%dT%H:%M").date(),
        "temperature_measure": "averge temperature"
    }

    print(temperature_data["date"])
    # Split the temperature data into three 8-hour shifts 
    shifts = [ data["hourly"]["temperature_2m"][0:8], # 00:00 to 07:00 
                data["hourly"]["temperature_2m"][8:16], # 08:00 to 15:00 
                data["hourly"]["temperature_2m"][16:24] # 16:00 to 23:00 
            ] 
    if all(temp is not None for shift in shifts for temp in shift):
        # Calculate the average temperature for each shift 
        average_temperatures = [sum(shift) / len(shift) for shift in shifts] 
        # Print the results 
        # for i, avg_temp in enumerate(average_temperatures): 
        #   print(f"Shift {i + 1} average temperature: {avg_temp:.2f} °C")
        temperature_data["morning"] = average_temperatures[0]
        temperature_data["afternoon"] = average_temperatures[1]
        temperature_data["evening"] = average_temperatures[2]
        print(temperature_data)
        try:
            # Establish connection
            conn = psycopg2.connect(**db_params)
            cursor = conn.cursor()
            insert_query = """ INSERT INTO temperature (date, morning, afternoon, evening) VALUES (%s, %s, %s, %s) """
            cursor.execute(insert_query, (temperature_data['date'], temperature_data['morning'], temperature_data['afternoon'], temperature_data['evening']))
            conn.commit()
            # Execute a query
            # Close the cursor and connection
            cursor.close()
            conn.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
    else:
        print(f"something is missing here the payload: {data}")

# Define a topic with chat messages in JSON format
messages_topic = app.topic(name="messages", value_deserializer="json")

# Create a StreamingDataFrame - the stream processing pipeline
# with a Pandas-like interface on streaming data
sdf = app.dataframe(topic=messages_topic)

# Print the input data
sdf = sdf.update(lambda message: insert_into_db(message))

# Run the streaming application
if __name__ == "__main__":
    app.run()