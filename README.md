### How to run this.
---   
1. Set up .env file with following keys. You can set values as per your requirement
````
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
````
2. Run db_creation.py file to create temperature table.   
````
python db_creation.py
````
3. Install and run Kafka and kafka UI using docker_compose file. You can also install locally from here [Kafka](https://kafka.apache.org/downloads). [Kafka-ui](https://github.com/provectus/kafka-ui).
````
docker-compose up -d
````
4. once Kafka install and running run the below command to start producer.
````
python producer.py
````
5. In another terminal run below command to start consumer.
````
python consumer.py
````
6. You can see Kafka dashborad here [http://localhost:8080/](http://localhost:8080/).
