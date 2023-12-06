# Real-Time-ETL
A python script fetches the data from the Yahoo! Finance API and provides it to the Kafka Producer at a regular interval to imitate a stream. The Kafka Producer then produces the data in a distributed system under a given topic. The Spark streaming is a distributed processing engine that will take care of running input streams incrementally and continuously and updating the final result as streaming data continues to arrive. It consumes data-streams from Kafka topic and preprocesses the data. Finally, the data is pushed to a Cassandra database.

![alt text](https://github.com/srblodhi/real-time-etl/blob/main/Data_Stream.svg?raw=true)

