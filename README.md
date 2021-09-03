# meetup-stream


prerequisites:
- Install kafka - follow https://kafka.apache.org/quickstart
- Configure your SQL SERVER settings in conf.ini
- Create data model in SQL SERVER according to data_model.sql

Setup: 
1) Start ZOOKEEPER
- cd C:\tools\kafka\kafka_2.12-2.8.0
- bin\windows\zookeeper-server-start.bat config\zookeeper.properties

2) Start KAFKA SERVER
- cd C:\tools\kafka\kafka_2.12-2.8.0
- bin\windows\kafka-server-start.bat config\server.properties

3) Start producer
- python producer.py

4) Start consumer
- python consumer.py




