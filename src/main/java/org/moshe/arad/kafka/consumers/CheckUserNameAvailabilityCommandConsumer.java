package org.moshe.arad.kafka.consumers;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.commands.CheckUserNameAvailabilityCommand;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.moshe.arad.service.UsersService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CheckUserNameAvailabilityCommandConsumer implements Runnable {

	Logger logger = LoggerFactory.getLogger(CheckUserNameAvailabilityCommandConsumer.class);
	private static final int CONSUMERS_NUM = 3;
	
	@Resource(name = "CheckUserNameAvailabilityCommandConfig")
	private SimpleConsumerConfig simpleConsumerConfig;
	
	private Consumer<String, CheckUserNameAvailabilityCommand> consumer;
	private boolean isRunning = true;
	private ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(6);
	
	@Autowired
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	@Autowired
	private UsersService usersService;
	
	public CheckUserNameAvailabilityCommandConsumer() {
		consumer = new KafkaConsumer<>(simpleConsumerConfig.getProperties());
	}

	private void executeConsumers(int numConsumers, String topicName){
		
		while(scheduledExecutor.getQueue().size() < numConsumers){
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			if(scheduledExecutor.getActiveCount() == numConsumers) continue;
			
			scheduledExecutor.scheduleAtFixedRate( () -> {
				consumer.subscribe(Arrays.asList(topicName));
	    		
	    		while (isRunning){
	                ConsumerRecords<String, CheckUserNameAvailabilityCommand> records = consumer.poll(100);
	                for (ConsumerRecord<String, CheckUserNameAvailabilityCommand> record : records){
	                	logger.info("Check User Name Availability Command record recieved, " + record.value());
//	                	usersService.checkUserNameAvailability(record.value().getUserName());
	                	//need to read snapshot from redis
	                }	              	             
	    		}
		        consumer.close();
		        
			} , 0, 100, TimeUnit.MILLISECONDS);
		}
	}
	
	@Override
	public void run() {
		this.executeConsumers(CONSUMERS_NUM, KafkaUtils.COMMANDS_TO_USERS_SERVICE_TOPIC);
	}

	public boolean isRunning() {
		return isRunning;
	}

	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}
	
	public ScheduledThreadPoolExecutor getScheduledExecutor() {
		return scheduledExecutor;
	}	
}




	