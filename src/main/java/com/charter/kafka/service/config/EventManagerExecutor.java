package com.charter.kafka.service.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class EventManagerExecutor 
{

	private static final Logger logger = LoggerFactory.getLogger(EventManagerExecutor.class);
	
	@Value("${eventmanager.thread.core-pool}")
	private int corePoolSize;
	
	@Value("${eventmanager.thread.max-pool}")
	private int maxPoolSize;
	
	@Value("${eventmanager.thread.queue.capacity}")
	private int queueCapacity;
	
	@Value("${eventmanager.thread.timeout}")
	private int threadTimeout;

	@Bean
	@Qualifier("eventManagerExecutor")
	public ThreadPoolTaskExecutor threadPoolTaskExecutor() 
	{
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		
		threadPoolTaskExecutor.setCorePoolSize(corePoolSize);
		threadPoolTaskExecutor.setMaxPoolSize(maxPoolSize);
		threadPoolTaskExecutor.setQueueCapacity(queueCapacity);
		threadPoolTaskExecutor.setKeepAliveSeconds(threadTimeout);
		threadPoolTaskExecutor.afterPropertiesSet();
		
		return threadPoolTaskExecutor;
	}
}