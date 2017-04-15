package org.moshe.arad.kafka.consumers;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.commands.CreateNewUserCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class CreateNewUserCommandConsumer implements Callable<Boolean>{

	Logger logger = LoggerFactory.getLogger(CreateNewUserCommandConsumer.class);
	
	private Properties properties;
	private Map<String,BackgammonUser> users;
	private Consumer<String, CreateNewUserCommand> consumer;
	private String topicName;
	
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

	public Map<String, BackgammonUser> getUsers() {
		return users;
	}

	public void setUsers(Map<String, BackgammonUser> users) {
		this.users = users;
	}

	@Override
	public Boolean call() throws Exception {
		try {
        	consumer.subscribe(Arrays.asList(topicName));
    		
    		while (true){
                ConsumerRecords<String, CreateNewUserCommand> records = consumer.poll(100);
                for (ConsumerRecord<String, CreateNewUserCommand> record : records){
                	logger.info("Create New User Command record recieved, " + record.value().getBackgammonUser());
                	users.put(record.value().getBackgammonUser().getUserName(), record.value().getBackgammonUser());
                	logger.info("users size is = " + users.size());
                }
                
                Thread.sleep(500);
    		}
			
		} catch (InterruptedException e) {
			logger.debug("An error occured while consuming Create New User Command record.");
			e.printStackTrace();
		}
        finally{
        	consumer.close();
        }
		return null;
	}	
}




	