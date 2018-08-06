# EventHubsJava
EventHub Simulation in Java

# Checkout the Source Files

Use the following command to grab the source files

```shell
git clone https://github.com/izzyacademy/EventHubsJava.git

```

# Setting up the Configurations
Create the configuration file from the example

Here you will define the namespace (Event Hub Cluster), the event hub (Topic), the identifier for the consumer group, Shared Access Signature Name and Key as well as the Storage Account Name and Key. These values will be used by the consumers and producers of events in our tests.

```shell

cd EventHubsJava

cp -p src/main/resources/config.properties.example src/main/resources/config.properties

```

In the config.properties file you can specify your own settings

# Compiling the Code

You can create the jar file for the project as follows

```shell

mvn clean package

```

# Running the Publisher (Producer)

You can run the producer as follows to generate events into the topic

```shell
java -jar target/eventhub-demo-32-shaded.jar com.izzyacademy.eventhubs.publishers.EventPublisher01 iekpo01
```

# Running the Consumers 

You can open multiple windows and run the consumers within the group as follows

Window 01

```shell
java -jar target/eventhub-demo-32-shaded.jar com.izzyacademy.enthubs.consumers.EventConsumer01 iekpo01

```

Window 02

```shell
java -jar target/eventhub-demo-32-shaded.jar com.izzyacademy.enthubs.consumers.EventConsumer01 iekpo02

```

Window 03

```shell
java -jar target/eventhub-demo-32-shaded.jar com.izzyacademy.enthubs.consumers.EventConsumer01 iekpo03

```
