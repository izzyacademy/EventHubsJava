package com.izzyacademy.eventhubs.consumers;

import static com.izzyacademy.eventhubs.core.ConfigConstants.EVENTHUB_CONSUMER_GROUP;
import static com.izzyacademy.eventhubs.core.ConfigConstants.EVENTHUB_NAMESPACE;
import static com.izzyacademy.eventhubs.core.ConfigConstants.EVENTHUB_SAS_KEY;
import static com.izzyacademy.eventhubs.core.ConfigConstants.EVENTHUB_SAS_NAME;
import static com.izzyacademy.eventhubs.core.ConfigConstants.EVENTHUB_SA_CONNECTION;
import static com.izzyacademy.eventhubs.core.ConfigConstants.EVENTHUB_SA_CONTAINER;
import static com.izzyacademy.eventhubs.core.ConfigConstants.EVENTHUB_TOPIC;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import com.izzyacademy.eventhubs.core.DriverTemplate;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.eventprocessorhost.PartitionManagerOptions;

/**
 * EventConsumer01
 *
 * java -jar target/eventhub-demo-32-shaded.jar com.izzyacadenthubs.consumers.EventConsumer01 iekpo01
 */
public class EventConsumer01 implements DriverTemplate {

	private int processEvents(String hostPrefix) throws Exception {
		
		final String configFileName = "config.properties";
		
		Properties properties = new Properties();
		InputStream inStream = null;
		
		// Grabs this number of events from the topic
		int prefetchCount = 32;
		
		// number of events passed to the event processor each time from EPH
		int maxBatchSize = 8;
		
		String eventHubNamespace = "";
		String eventHubTopic = "";
		String eventHubSasName = "";
		String eventHubSasKey = "";
		
		String eventHubConsumerGroup = "";
		String eventHubStorageConnectionString = "";
		String eventHubStorageContainerName = "";
		
		
		
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		
		inStream = loader.getResourceAsStream(configFileName);
		
		if (inStream == null) {
			
			throw new RuntimeException("Unable to locate configuration file " + configFileName);
		}
		
		// Load up the configuration file contents into the properties object
		try {
			
			properties.load(inStream);
			
			eventHubNamespace = properties.getProperty(EVENTHUB_NAMESPACE);
			eventHubTopic = properties.getProperty(EVENTHUB_TOPIC);
			eventHubSasName = properties.getProperty(EVENTHUB_SAS_NAME);
			eventHubSasKey = properties.getProperty(EVENTHUB_SAS_KEY);
			
			eventHubConsumerGroup = properties.getProperty(EVENTHUB_CONSUMER_GROUP);
			eventHubStorageConnectionString = properties.getProperty(EVENTHUB_SA_CONNECTION);
			eventHubStorageContainerName = properties.getProperty(EVENTHUB_SA_CONTAINER);
			
		} catch (IOException e) {
			
			throw new RuntimeException("Unable to load configuration from " + configFileName, e);
			
		} finally {
			
		}
		
		ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder();
		
		// Constructing the Connection String to the EventHub Name space (Cluster)
		connectionStringBuilder.setNamespaceName(eventHubNamespace)
							   .setEventHubName(eventHubTopic)
							   .setSasKeyName(eventHubSasName)
							   .setSasKey(eventHubSasKey);
		
		String eventHubConnectionString = connectionStringBuilder.toString();
		
		System.out.println("Connection String = " + eventHubConnectionString);
		
		EventProcessorHost host = new EventProcessorHost(
				EventProcessorHost.createHostName(hostPrefix), // hostname must be unique per consumer instance
				eventHubTopic,
				eventHubConsumerGroup,
				eventHubConnectionString,
				eventHubStorageConnectionString,
				eventHubStorageContainerName);
		
		System.out.println("Registering EPH host named " + host.getHostName());
		
		EventProcessorOptions options = new EventProcessorOptions();
		
		options.setExceptionNotification(new ErrorNotificationHandler());
		options.setPrefetchCount(prefetchCount);
		options.setMaxBatchSize(maxBatchSize);
		
		PartitionManagerOptions partitionMgrOptions = host.getPartitionManagerOptions();
		
		partitionMgrOptions.setLeaseDurationInSeconds(20);
		partitionMgrOptions.setLeaseRenewIntervalInSeconds(5);
		
		CompletableFuture<Void> completeableFuture = host.registerEventProcessor(EventProcessor.class, options);
		
		completeableFuture.whenComplete((unused, e) ->
		{
			// whenComplete passes the result of the previous stage through unchanged,
			// which makes it useful for logging a result without side effects.
			if (e != null)
			{
				System.out.println("Failure while registering: " + e.toString());
				if (e.getCause() != null)
				{
					System.out.println("Inner exception: " + e.getCause().toString());
				}
			}
			
		}).thenAccept((unused) ->
		{
			// This stage will only execute if registerEventProcessor succeeded.
			// If it completed exceptionally, this stage will be skipped.
		})
		.thenCompose((unused) ->
		{
			// This stage will only execute if registerEventProcessor succeeded.
			//
            // Processing of events continues until unregisterEventProcessor is called. Unregistering shuts down the
            // receivers on all currently owned leases, shuts down the instances of the event processor class, and
            // releases the leases for other instances of EventProcessorHost to claim.
			System.out.println("Shutting down EPH ...");
			return completeableFuture;
			//return host.unregisterEventProcessor();
		})
		.exceptionally((e) ->
		{
			System.out.println("Failure while unregistering: " + e.toString());
			
			if (e.getCause() != null)
			{
				System.out.println("Inner exception: " + e.getCause().toString());
			}
			return null;
		})
		.get(); // Wait for everything to finish before exiting.
		
		System.out.println("....................Consumer has returned from processing .............................");
		
		return 0;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		String hostPrefix = args[1];
		
		return processEvents(hostPrefix);
	}
}