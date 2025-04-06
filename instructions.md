README


The code is in the git repository that we add the link to in the document.

It will be necessary to clone it and install the necessary requirements.

For the testing of the code it is necessary the .env file we add to this zip file. 
It contains the necessary passwords to run some of the pipelines.

# BDM_Project
Big Data Management  - Project

## Kafka Setup and Topic Management

### Step-by-Step Instructions

1. Navigate to the directory where Kafka is installed.

2. Start Zookeeper in a terminal:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

3. Start Kafka in a separate terminal:
```bash
bin/kafka-server-start.sh config/server.properties
```

4. In another terminal, create your desired Kafka topic. Example:
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bluesky_posts --partitions 1 --replication-factor 1
```

5. To list all available topics:
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

6. To listen to messages from a topic:
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bluesky_posts --from-beginning
```

---

### Create Kafka Topics

#### Create `emissions_data`
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic emissions_data --partitions 1 --replication-factor 1
```

#### Create `gencat_incidents`
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic gencat_incidents --partitions 1 --replication-factor 1
```

#### Create `renfe_incidents`
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic renfe_incidents --partitions 1 --replication-factor 1
```

---

### Listen to Kafka Topics

#### Listen to `emissions_data`
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic emissions_data --from-beginning
```

#### Listen to `gencat_incidents`
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic gencat_incidents --from-beginning
```

#### Listen to `renfe_incidents`
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic renfe_incidents --from-beginning
```


We have also created a main.bsh to test the different data sources.
We are working on orchestrating everything with airflow and dockerizing it for a better user experience.
