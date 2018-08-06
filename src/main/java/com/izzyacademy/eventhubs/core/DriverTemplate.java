package com.izzyacademy.eventhubs.core;

public interface DriverTemplate {

	/**
	 * Accepts an array of String Commands
	 * 
	 * Returns 1 on error and 0 on success
	 * 
	 * @param arguements
	 * 
	 * @return Status of the job
	 */
	int run(String[] arguements) throws Exception;
	
}
