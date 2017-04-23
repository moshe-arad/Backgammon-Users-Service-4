package org.moshe.arad.kafka.producers.commands;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.Commandable;
import org.moshe.arad.kafka.commands.PullEventsCommand;
import org.moshe.arad.kafka.producers.config.SimpleProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
/**
 * 
 * @author moshe-arad
 *
 * @param <T> is the event that we want to pass
 * 
 * important to set topic and properties before usage
 */
@Component
@Scope("prototype")
public abstract class SimpleBackgammonCommandsProducer <T extends Commandable> implements SimpleProducer, Runnable {

	private final Logger logger = LoggerFactory.getLogger(SimpleBackgammonCommandsProducer.class);
	
	private SimpleProducerConfig simpleProducerConfig;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	private ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(6);
	private boolean isRunning = true;
	private static final int PRODUCERS_NUM = 3;
	private String topic;
	
	private static final int DELAY_MIN = 10;
	
	public SimpleBackgammonCommandsProducer() {
	}
	
	public SimpleBackgammonCommandsProducer(SimpleProducerConfig simpleProducerConfig, String topic) {
		this.simpleProducerConfig = simpleProducerConfig;
		this.topic = topic;
	}
	
	@Override
    public void sendKafkaMessage(Commandable command){
		try{
			logger.info("Front Service is about to send a Command to topic=" + topic + ", Event=" + command);
			sendMessage(command);
			logger.info("Message sent successfully, Front Service sent a Command to topic=" + topic + ", Event=" + command);
		}
		catch(Exception ex){
			logger.error("Failed to sent message, Front Service failed to send a Command to topic=" + topic + ", Event=" + command);
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	private void sendMessage(Commandable command){
		logger.info("Creating kafka producer.");
		Producer<String, Commandable> producer = new KafkaProducer<>(simpleProducerConfig.getProperties());
		logger.info("Kafka producer created.");
		
		logger.info("Sending message to topic = " + topic + ", message = " + command.toString() + ".");
		ProducerRecord<String, Commandable> record = new ProducerRecord<String, Commandable>(topic, command);
		producer.send(record);
		logger.info("Message sent.");
		producer.close();
		logger.info("Kafka producer closed.");
	}

	@SuppressWarnings("unchecked")
	private void takeMessagesFromConsumersAndPass(int numJobs){
		while(scheduledExecutor.getQueue().size() < numJobs){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
			if(scheduledExecutor.getActiveCount() == numJobs) continue;
			
			scheduledExecutor.scheduleAtFixedRate(() -> {
				while(isRunning){
					//need to ask redis for a desired date
					PullEventsCommand pullEventsCommand = new PullEventsCommand(UUID.randomUUID(), new Date());
					sendKafkaMessage(pullEventsCommand);
				}
			}, 0, DELAY_MIN, TimeUnit.MINUTES);
		}
	}
	
	public abstract void eventsSchedulerOperations();
	
	public boolean isRunning() {
		return isRunning;
	}

	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}

	public ScheduledThreadPoolExecutor getScheduledExecutor() {
		return scheduledExecutor;
	}

	@Override
	public void run() {
		this.takeMessagesFromConsumersAndPass(PRODUCERS_NUM);		
	}

	public SimpleProducerConfig getSimpleProducerConfig() {
		return simpleProducerConfig;
	}

	public void setSimpleProducerConfig(SimpleProducerConfig simpleProducerConfig) {
		this.simpleProducerConfig = simpleProducerConfig;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public ConsumerToProducerQueue getConsumerToProducerQueue() {
		return consumerToProducerQueue;
	}

	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}	
}
