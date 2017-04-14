package org.moshe.arad.kafka.consumers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.commands.CreateNewUserCommand;
import org.springframework.stereotype.Component;

@Component
public class CreateNewUserCommandConsumer {

	private Properties properties;
	private Map<String,BackgammonUser> users;
	
	public CreateNewUserCommandConsumer() {
		properties = new Properties();
		properties.put("bootstrap.servers", "192.168.1.10:9092,192.168.1.10:9093,192.168.1.10:9094");
		properties.put("group.id", "CreateNewUserCommandGroup");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.moshe.arad.kafka.deserializers.CreateNewUserCommandDeserializer");										      
	}
	
	public CreateNewUserCommandConsumer(String customValueDeserializer, String groupName, Map<String,BackgammonUser> users) {
		properties = new Properties();
		properties.put("bootstrap.servers", "192.168.1.10:9092,192.168.1.10:9093,192.168.1.10:9094");
		properties.put("group.id", groupName);
		properties.put("key.deserializer", "org.apache.kafka.common.deserialization.StringDeserializer");
		properties.put("value.deserializer", customValueDeserializer);
		this.users = users;
	}
	
	public void recieveKafkaMessage(String topicName){
		Consumer<String, CreateNewUserCommand> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(topicName));
		
		while (true){
            ConsumerRecords<String, CreateNewUserCommand> records = consumer.poll(100);
            for (ConsumerRecord<String, CreateNewUserCommand> record : records){
            	System.out.println("Create New User Command record recieved, " + record.value().getBackgammonUser());
            	users.put(record.value().getBackgammonUser().getUserName(), record.value().getBackgammonUser());
            	System.out.println("users size is = " + users.size());
            }
            
            try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public Map<String, BackgammonUser> getUsers() {
		return users;
	}

	public void setUsers(Map<String, BackgammonUser> users) {
		this.users = users;
	}	
}




	