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

# How Everything Comes Together

In my test, I set up a namespace (cluster) with 1 Event Hub (topic) that has 32 partitions (this is the current max)

I defined a specific consumer group within this topic.
https://github.com/izzyacademy/EventHubsJava/blob/master/src/main/resources/config.properties.example

https://github.com/izzyacademy/EventHubsJava/blob/master/src/main/java/com/izzyacademy/eventhubs/publishers/EventPublisher01.java 

I was able to write data to the topic without specifying the partition keys. In this scenario it round robins to all partitions within the topic.

If I specify the partition key, then it consistently put the message in the same partition. 

There may be use cases for stateful analysis where you might want to keep them in the same partition.

With EPH (com.microsoft.azure.eventprocessorhost.EventProcessorHost), I used 3 consumers within my consumer group.

https://github.com/izzyacademy/EventHubsJava/blob/master/src/main/java/com/izzyacademy/eventhubs/consumers/EventConsumer01.java

With EPH you need an EventProcessor - https://github.com/izzyacademy/EventHubsJava/blob/master/src/main/java/com/izzyacademy/eventhubs/consumers/EventProcessor.java

This is a class that implements com.microsoft.azure.eventprocessorhost.IEventProcessor. The EventProcessor object is where all the work happens.

Its onEvents() method receives a list of events which you have to loop through and process on at a time. 

You have to call the checkpoint(data) method on the context to do a checkpoint (commit the offset in azure storage account) and then you need to call get() on the checkpoint method on the CompletableFuture object to ensure that it blocks and returns before we proceed from the checkpoint.

Once all the consumers within the group were active, EPH acquired a lease on the partition(s) for each consumer within the group.

While the lease is active, the other group members do not read from this partitions. Each consumer has its own subset of partitions that are exclusive.

If the consumer dies or is terminated, the lease on the partitions are released and can be re-assigned evenly to other consumers in the group.

When the consumer rejoins the group, it will have to wait for the leases to expire so that it can be redistributed evenly amongst the members. 

The lease period length is configurable.

```java
EventProcessorOptions options = new EventProcessorOptions();
             
            options.setExceptionNotification(new ErrorNotificationHandler());
            options.setPrefetchCount(prefetchCount);
            options.setMaxBatchSize(maxBatchSize);
             
             PartitionManagerOptions partitionMgrOptions = host.getPartitionManagerOptions();
             
      partitionMgrOptions.setLeaseDurationInSeconds(20);
      partitionMgrOptions.setLeaseRenewIntervalInSeconds(5);
             
             CompletableFuture<Void> completeableFuture = host.registerEventProcessor(EventProcessor.class, options);

```

When it resumes processing, it picks off from a location after the last committed offset when the checkpoint was made.

