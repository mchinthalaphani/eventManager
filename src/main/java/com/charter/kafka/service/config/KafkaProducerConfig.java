package com.charter.kafka.service.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.charter.eni.model.Msg;
import com.charter.eni.model.OrderDetailUpdated.AccountList.Account;
import com.charter.kafka.service.db.impl.KafkaSoloSyncDbImpl;
import com.charter.solosync.db.connector.SolosyncMessageConnector;
import com.charter.solosync.db.model.SolosyncMessage;

@Component
public class KafkaProducerConfig 
{

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerConfig.class);
	
	protected static final String ACCOUNT = "Account";
	protected static final String LOC = "Location";
	protected static final String CUST = "Customer";
	protected static final String ORDER = "Order";
	protected static final String ACCT_CURR_SVCS = "AccountCurrentServices";
	
	protected static final String ACCT_DETAIL_UPDT = "AccountDetailUpdated";
	protected static final String ACCT_ADDED = "AccountAdded";
	protected static final String ACCT_UPDT = "AccountUpdated";
	protected static final String CUST_UPDT = "CustomerUpdated";
	protected static final String LOC_ADDED = "LocationAdded";
	protected static final String LOC_UPDT = "LocationUpdated";
	protected static final String ORD_DETAIL_UPDT = "OrderDetailUpdated";
	protected static final String ACCT_SEASONAL_NOTIF = "AccountSeasonalNotifications";
	
	
	public static final Random generator = new Random(); // Thread-safe random number generation
	
	private final Properties properties = new Properties();
	KafkaProducer<String, Object> producer;
	
	@Autowired
	private KafkaSoloSyncDbImpl dbImpl;
	
	@Value("${bootstrap.servers}")
	private String brokerList;

	@Value("${acks}")
	private String acks;

	@Value("${retries}")
	private String retries;

	@Value("${linger.ms}")
	private String linger;

	@Value("${batch.size}")
	private String size;

	@Value("${block.on.buffer.full}")
	private String buffer;

	@Value("${auto.commit.interval.ms}")
	private String autoCommit;
	
// CSG Topics
//	@Value("${topic_name}")
//	private String topic;
	
	@Value("${account_topic}")
	private String accountTopic;
	
	@Value("{account_current_svcs_topic}")
	private String accountCurrSvcsTopic;
	
	@Value("${location_topic}")
	private String locationTopic;
	
	@Value("${customer_topic}")
	private String customerTopic;
	
	@Value("${order_topic}")
	private String orderTopic;

	public KafkaProducerConfig() 
	{

	}

	@PostConstruct
	public void init() 
	{
		properties.put("metadata.broker.list", brokerList);
		properties.put("bootstrap.servers", brokerList);
		properties.put("acks", acks); //Research more For Kafka to EventManager/Producer Acknowledgement - HPatel
		properties.put("retries", retries);
		properties.put("linger.ms", linger);
		properties.put("batch.size", size);
		properties.put("serializer.class", "com.charter.kafka.service.config.XmlEncoder");
		//properties.put("partitioner.class", "com.charter.kafka.service.config.KafkaPartitioner"); // Custom Partition Class - HPatel
		properties.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.charter.kafka.service.config.ObjectSerializer");
		properties.put("block.on.buffer.full", buffer);
		properties.put("auto.commit.interval.ms", autoCommit);
		properties.put("account", accountTopic);
		properties.put("accountCurrSvcs", accountCurrSvcsTopic);
		properties.put("location", locationTopic);
		properties.put("customer", customerTopic);
		properties.put("order", orderTopic);
		
	//	ProducerConfig prodConfig = new ProducerConfig(properties);
		producer = new KafkaProducer<String, Object>(properties);
		
		/*logger.debug("KafkaConnection.metadata.broker.list: " +brokerList);
		logger.debug("KafkaConnection.acks: " + acks);
		logger.debug("KafkaConnection.retries: " + retries);
		logger.debug("KafkaConnection.linger.ms: " + linger);
		logger.debug("KafkaConnection.batch.size: " + size);
		logger.debug("KafkaConnection.serializer.class: " +"com.charter.kafka.service.config.XmlEncoder");
		logger.debug("KafkaConnection.block.on.buffer.full: " + buffer);
		logger.debug("KafkaConnection.auto.commit.interval.ms: " +autoCommit);*/
		
		System.out.println("KafkaConnection.metadata.broker.list: " +brokerList);
		System.out.println("KafkaConnection.acks: " + acks);
		System.out.println("KafkaConnection.retries: " + retries);
		System.out.println("KafkaConnection.linger.ms: " + linger);
		System.out.println("KafkaConnection.batch.size: " + size);
		System.out.println("KafkaConnection.serializer.class: " + "com.charter.kafka.service.config.XmlEncoder");
		System.out.println("KafkaConnection.block.on.buffer.full: " + buffer);
		System.out.println("KafkaConnection.auto.commit.interval.ms: " + autoCommit);

	}

	/**
	 * A method that sends xml payload to Kafka for the consumer to consume it.
	 * 
	 * @param message
	 */
	public void send(Object message) 
	{
		logger.info("send() method.");
		String triggerType = getTriggerType(message);
		String transactionId = generateId();
		List<String> acctNumbers = getAccountNumbers(message); 
		logger.info("AccountIds = " +acctNumbers.toString()); //log it for Splunk reports.
		
		sendToKafka(message, triggerType, transactionId);
		logger.info("EXIT send() method.");
	}
	
	/**
	 * Extract TriggerType from payload
	 * 
	 * @param payload
	 * @return type
	 */
	
	private String getTriggerType(Object payload) 
	{
		Msg msg = (Msg) payload;
		String type = msg.getHead().getTriggerType();
		
		return type;
	}
	
	/**
	 * Method to extract AccountNumbers from xml payload. 
	 * Extracting AccountNumber here for Splunk Log reporting.
	 * 
	 * @param payload
	 * @return acctIds
	 * 
	 */
	private List<String> getAccountNumbers(Object payload) 
	{
		logger.info("getAccountNumber() - for Splunk reporting. ");
		Msg msg = (Msg) payload;
		List<Account> accountList = new ArrayList<Account>();
		List<String> acctIds = new ArrayList<String>();
		
		if (msg != null && msg.getBody().getOrderDetailUpdated().getAccountList() != null) 
		{
			accountList = msg.getBody().getOrderDetailUpdated().getAccountList().getAccount();

			for (Account account : accountList) 
			{
				if (account != null && account.getAccountId() != null) 
				{
					acctIds.add(String.valueOf(account.getAccountId()));
				}
			}
		}
		return acctIds;
	}
	
	/**
	 * A private method that sends xml message to Kafka Topic
	 * 
	 * @param msg
	 * @param triggerType
	 * @param transId
	 * 
	 */
	private void sendToKafka(Object msg, String triggerType, String transId) 
	{
		long startTime = System.currentTimeMillis();
		
		dbImpl.saveSoloSyncMessageStatus(transId,"IN_PRODUCER");
		
		logger.info("SendToKafka() method.");
		KeyedMessage<String, Object> keyedMsg = null;
		
		
		if (ACCT_ADDED.equalsIgnoreCase(triggerType) || 
		    ACCT_UPDT.equalsIgnoreCase(triggerType))
		{
			keyedMsg = new KeyedMessage<String, Object>(accountTopic, transId, msg);
			logger.info("KafkaProducerConfig.Account [" + accountTopic +", "+ transId +"]");
			//producer.send(keyedMsg);
			producer.send(new ProducerRecord<>(accountTopic,
                    transId,
                    msg), new DemoCallBack(startTime,transId, msg,dbImpl));
		}  
		if (ACCT_DETAIL_UPDT.equalsIgnoreCase(triggerType) || 
			    ACCT_SEASONAL_NOTIF.equalsIgnoreCase(triggerType))
		{
			keyedMsg = new KeyedMessage<String, Object>(accountCurrSvcsTopic, transId, msg);
			logger.info("KafkaProducerConfig.CurrentServices [" + accountCurrSvcsTopic +", "+ transId +"]");
			//producer.send(keyedMsg);
			producer.send(new ProducerRecord<>(accountCurrSvcsTopic,
                    transId,
                    msg), new DemoCallBack(startTime,transId, msg,dbImpl));
		} 
		if (CUST_UPDT.equalsIgnoreCase(triggerType))
		{
			keyedMsg = new KeyedMessage<String, Object>(customerTopic, transId, msg);
			logger.info("KafkaProducerConfig.Customer [" + customerTopic +", "+ transId +"]");
			//producer.send(keyedMsg);
			producer.send(new ProducerRecord<>(customerTopic,
                    transId,
                    msg), new DemoCallBack(startTime,transId, msg, dbImpl));
			
		} 
		if (ORD_DETAIL_UPDT.equalsIgnoreCase(triggerType)) 
		{
			keyedMsg = new KeyedMessage<String, Object>(orderTopic, transId, msg);
			logger.info("KafkaProducerConfig.Order [" + orderTopic +", "+ transId +"]");

			producer.send(new ProducerRecord<>(orderTopic,
                    transId,
                    msg), new DemoCallBack(startTime,transId, msg, dbImpl));
		}
		
		if (LOC_ADDED.equalsIgnoreCase(triggerType) ||
		    LOC_UPDT.equalsIgnoreCase(triggerType)) 
		{
			keyedMsg = new KeyedMessage<String, Object>(locationTopic, transId, msg);
			logger.info("KafkaProducerConfig.Location [" + locationTopic +", "+ transId +"]");
			//producer.send(keyedMsg);
			producer.send(new ProducerRecord<>(locationTopic,
                    transId,
                    msg), new DemoCallBack(startTime,transId, msg, dbImpl));
		} 
		
	//	logger.info("Message = ["+keyedMsg.toString()+"]");
		logger.info("Exiting SendToKafka() method.");
		
	}
	
	/**
	 * Generate random transactionId using either UUID java class or thread-safe random number generation.
	 * 
	 * @param 
	 * @return String value
	 * 
	 */
	public static final String generateId()
	{
	   return String.valueOf(UUID.randomUUID());
		//return (long) generator.nextLong()+""; // Thread-safe random number generation
	   
	}
	 
}

class DemoCallBack implements Callback {
	
	
	private KafkaSoloSyncDbImpl dbImpl;
	
    private final long startTime;
    private final String key;
    private final Object message;

    public DemoCallBack(long startTime, String key, Object message, KafkaSoloSyncDbImpl dbImpl) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
        this.dbImpl = dbImpl;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
        	dbImpl.saveSoloSyncMessageStatus(key,"IN_KAFKA");
            System.out.println(
                "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            
        } else {
        	dbImpl.saveSoloSyncMessageStatus(key,"FAILED_TO_REACH_KAFKA");
            exception.printStackTrace();
        }
    }
}


