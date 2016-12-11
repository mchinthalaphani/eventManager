package com.charter.kafka.service.util;

import java.util.Random;

public class RandomGen 
{
	
	private static void generateRandomNumber(long aStart, long aEnd, Random aRandom) {
		if (aStart > aEnd) {
			throw new IllegalArgumentException("Start cannot exceed End.");
		}
		// get the range, casting to long to avoid overflow problems
		long range = (long) aEnd - (long) aStart + 1;
		long randomNumber = (long) (range * aRandom.nextDouble());
	}

	private static void log(String aMessage) {
		System.out.println(aMessage);
	}
}
