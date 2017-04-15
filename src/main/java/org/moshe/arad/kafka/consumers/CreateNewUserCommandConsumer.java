package org.moshe.arad.kafka.consumers;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.commands.CreateNewUserCommand;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.EventFactory;
import org.moshe.arad.kafka.events.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CreateNewUserCommandConsumer implements Runnable {

	Logger logger = LoggerFactory.getLogger(CreateNewUserCommandConsumer.class);
	private static final int CONSUMERS_NUM = 3;
	private Properties properties;
	private Map<String,BackgammonUser> users;
	private Consumer<String, CreateNewUserCommand> consumer;
	private String topicName;
	private boolean isRunning = true;
	private ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(6);
	
	@Autowired
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	public CreateNewUserCommandConsumer() {
		properties = new Properties();
		properties.put("bootstrap.servers", KafkaUtils.SERVERS);
		properties.put("group.id", KafkaUtils.CREATE_NEW_USER_COMMAND_GROUP);
		properties.put("key.deserializer", KafkaUtils.KEY_STRING_DESERIALIZER);
		properties.put("value.deserializer", KafkaUtils.CREATE_NEW_USER_COMMAND_DESERIALIZER);
		consumer = new KafkaConsumer<>(properties);
	}
	
	public CreateNewUserCommandConsumer(String customValueDeserializer, String groupName, Map<String,BackgammonUser> users, String topicName) {
		properties = new Properties();
		properties.put("bootstrap.servers", KafkaUtils.SERVERS);
		properties.put("group.id", groupName);
		properties.put("key.deserializer", KafkaUtils.KEY_STRING_DESERIALIZER);
		properties.put("value.deserializer", customValueDeserializer);
		this.users = users;
		this.topicName = topicName;
		consumer = new KafkaConsumer<>(properties);
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
			
			logger.info("Threads in pool's queue before schedule = " + scheduledExecutor.getQueue().size());
			scheduledExecutor.scheduleAtFixedRate( () -> {
				consumer.subscribe(Arrays.asList(topicName));
	    		
	    		while (isRunning){
	                ConsumerRecords<String, CreateNewUserCommand> records = consumer.poll(100);
	                for (ConsumerRecord<String, CreateNewUserCommand> record : records){
	                	logger.info("Create New User Command record recieved, " + record.value().getBackgammonUser());
	                	users.put(record.value().getBackgammonUser().getUserName(), record.value().getBackgammonUser());	            
	                	BackgammonEvent newUserCreatedEvent = EventFactory.getEvent(Events.NewUserCreatedEvent, record.value().getBackgammonUser());
	                	consumerToProducerQueue.getEventsQueue().put(newUserCreatedEvent);
	                	logger.info("users size is = " + users.size());
	                }	              	             
	    		}
		        consumer.close();
		        
			} , 0, 100, TimeUnit.MILLISECONDS);
			logger.info("Threads in pool's queue after schedule = " + scheduledExecutor.getQueue().size());
		}
	}
	
	@Override
	public void run() {
		this.executeConsumers(CONSUMERS_NUM, KafkaUtils.COMMANDS_TO_USERS_SERVICE_TOPIC);
	}
	
	public Map<String, BackgammonUser> getUsers() {
		return users;
	}

	public void setUsers(Map<String, BackgammonUser> users) {
		this.users = users;
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




	