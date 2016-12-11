package com.charter.kafka.service.db.impl;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.charter.eni.model.Msg;
import com.charter.solosync.db.connector.SolosyncMessageConnector;
import com.charter.solosync.db.model.SolosyncMessage;

@Component
public class KafkaSoloSyncDbImpl {

	private static final Logger logger = LoggerFactory.getLogger(KafkaSoloSyncDbImpl.class);
	
	protected static final String IN_PRODUCER = "IN_PRODUCER";
	protected static final String IN_KAFKA = "IN_KAFKA";
	
	@Autowired
	private SolosyncMessageConnector soloSyncMessage;
		
	//public static final Random generator = new Random(); // Thread-safe random number generation

	public Long saveSoloSyncMessageStatus(String tid,String status) {

		SolosyncMessage soloMsg = new SolosyncMessage();
		soloMsg.setSolosyncTransId(tid);
		soloMsg.setCsgTimestamp(new java.sql.Date(new java.util.Date().getTime()));  // Set CSGTimeStammp - HPatel
		soloMsg.setMessageStatus(status);
		logger.info("Transaction Id in KafkaSoloSyncDbImpl =["+tid+"]");
		Long msgId  = soloSyncMessage.save(soloMsg);
		
		return msgId;
	}
	
//	public static final Long generateId()
//	{
//	  // return String.valueOf(UUID.randomUUID());
//		return (long) generator.nextLong(); // Thread-safe random number generation
//	}

}
