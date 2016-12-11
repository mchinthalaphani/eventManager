package com.charter.kafka.service.controller;

import javax.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.charter.eni.model.Msg;
import com.charter.kafka.service.config.EventManagerExecutor;
import com.charter.kafka.service.config.KafkaProducerConfig;

@Controller
public class KafkaController {

	@Autowired
	KafkaProducerConfig producer;
	
	@Autowired
	EventManagerExecutor eventManagerExecutor;

	private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
	
	@SuppressWarnings("finally")
	@RequestMapping(method = { RequestMethod.POST }, value = "/eventManager", consumes = MediaType.APPLICATION_XML_VALUE)
	@Async("eventManagerExecutor")
	public @ResponseBody int processRequest(@RequestBody @Valid final Msg message)
	{
		logger.info("Entering KafkaController.processRequest() method.");
		
		int returnCode = 200;
		if(message instanceof Msg) 
		{
			try 
			{
				logger.info("Before producer.send() for kafka topic :" + message.toString());
				producer.send(message);
				logger.info("processRequest() --> Message sent to Consumer from Producer..");
				
			} catch (Exception e) 
			{
				e.printStackTrace();
				returnCode = 500;
			} finally 
			{
				return returnCode;
			}
		} 
		else 
		{
			throw new IllegalArgumentException("Message must be of type Msg.");
		}
		
	}
	
}