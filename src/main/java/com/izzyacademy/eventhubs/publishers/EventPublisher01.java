package com.izzyacademy.eventhubs.publishers;

import static com.izzyacademy.eventhubs.core.ConfigConstants.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.izzyacademy.eventhubs.core.DriverTemplate;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;

/**
 * 
 * java -jar target/eventhub-demo-32-shaded.jar com.izzyacademy.eventhubs.publishers.EventPublisher01 iekpo01
 *
 */
public class EventPublisher01 implements DriverTemplate {

	@Override
	public int run(String[] arguements) throws Exception {
		
		final int totalNumberOfMessages = 64 * 32;
		
		final String configFileName = "config.properties";
		
		Properties properties = new Properties();
		InputStream inStream = null;
		
		String eventHubNamespace = "";
		String eventHubTopic = "";
		String eventHubSasName = "";
		String eventHubSasKey = "";
		
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
		
		String connectionString = connectionStringBuilder.toString();
		
		System.out.println("Connection String = " + connectionString);
		
		final ExecutorService executorService = Executors.newSingleThreadExecutor();
		
		final EventHubClient eventHubClient = EventHubClient.createSync(connectionString, executorService);
		
		Gson gson = new GsonBuilder().create();
		
		try {
			
			// Send messages on at a time
			for (int rowNumber=0; rowNumber < totalNumberOfMessages; ++rowNumber) {
				
				//String partitionKey = Integer.toString(rowNumber);
				
				// This is the message we are generating
				String messagePayload = Integer.toString(rowNumber) + "," + System.currentTimeMillis();
				
				// Serialize the event into bytes
				byte[] payloadBytes = gson.toJson(messagePayload).getBytes(Charset.defaultCharset());
				
				// Use the bytes to construct an {@link EventData} object
				EventData sendEvent = EventData.create(payloadBytes);
				
				// Transmits the event to event hub
				// If a partition key is not set, then we will round-robin the {@link EventData}'s to all topic partitions
				eventHubClient.sendSync(sendEvent);
				
				//  the partitionKey will be hash'ed to determine the partitionId to send the eventData to.
				//eventHubClient.sendSync(sendEvent, partitionKey);
				
				System.out.println("Sent Message " + messagePayload);
			}
			
			System.out.println(Instant.now() + ": Publishing Complete. " + totalNumberOfMessages + " messages transmitted");
			
		} finally {
			
			eventHubClient.closeSync();
			executorService.shutdown();
		}
		
		return 0;
	}

}