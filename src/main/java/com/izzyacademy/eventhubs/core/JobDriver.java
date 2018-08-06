package com.izzyacademy.eventhubs.core;

public class JobDriver {

	private static final String EVENT_PUBLISHER = "com.izzyacademy.eventhubs.publishers.EventPublisher01";
	
	private static final String EVENT_CONSUMER_01 = "com.izzyacademy.eventhubs.consumers.EventConsumer01";
	
	public static void main(String[] args) throws Exception {
		
		//runEventPublisher01();
		
		//runEventConsumer01();
		
		if (args.length != 2) {
			
			System.err.println("Usage: jobRunnerFQCN hostPrefix");
			System.err.println("com.izzyacademy.eventhubs.consumers.EventConsumer01 iekpo01");
			System.err.println("com.izzyacademy.eventhubs.publishers.EventPublisher01 iekpo01");
			
			throw new IllegalArgumentException("Usage: jobRunnerFQCN hostPrefix");
		}
		
		String jobRunnerClassName = args[0];
		
		DriverTemplate driver = JobDriver.class.getClassLoader().loadClass(jobRunnerClassName).asSubclass(DriverTemplate.class).newInstance();
		
		driver.run(args);
	}
	
	protected static void runEventPublisher01() throws Exception {
		
		String driverName = EVENT_PUBLISHER;
		
		DriverTemplate driver = JobDriver.class.getClassLoader().loadClass(driverName).asSubclass(DriverTemplate.class).newInstance();
		
		String[] args = {};
		
		driver.run(args);
	}
	
	protected static void runEventConsumer01() throws Exception {
		
		String driverName = EVENT_CONSUMER_01;
		
		DriverTemplate driver = JobDriver.class.getClassLoader().loadClass(driverName).asSubclass(DriverTemplate.class).newInstance();
		
		String[] args = {"", "isekpo01"};
		
		driver.run(args);
	}
}
