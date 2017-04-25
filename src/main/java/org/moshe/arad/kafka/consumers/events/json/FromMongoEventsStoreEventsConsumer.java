package org.moshe.arad.kafka.consumers.events.json;

import java.util.LinkedList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.kafka.consumers.SimpleConsumerConfig;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class FromMongoEventsStoreEventsConsumer extends SimpleBackgammonEventsConsumer {
	
	@Autowired
	private SnapshotAPI snapshotAPI;
	
	Logger logger = LoggerFactory.getLogger(FromMongoEventsStoreEventsConsumer.class);
	
	private boolean isReadingEvents = false;
	
	private LinkedList<BackgammonEvent> fromMongoEventsStoreEventList = new LinkedList();
	
	public FromMongoEventsStoreEventsConsumer() {
	}
	
	public FromMongoEventsStoreEventsConsumer(SimpleConsumerConfig simpleConsumerConfig, String topic) {
		super(simpleConsumerConfig, topic);
	}

	@Override
	public void consumerOperations(ConsumerRecord<String,String> record) {
		try{
			logger.info("Trying to convert JSON blob to Backgammon Event record, JSON blob = " + record.value());
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode jsonNode = objectMapper.readValue(record.value(), JsonNode.class);
			String clazz = jsonNode.get("clazz").asText();
			BackgammonEvent backgammonEvent = null;
			
			if(clazz.equals("NewUserCreatedEvent"))
			{
				NewUserCreatedEvent newUserCreatedEvent = objectMapper.readValue(record.value(), NewUserCreatedEvent.class);
				
				if(newUserCreatedEvent.getServiceId() == -1) {
					logger.info("Recieved the begin read events record, starting reading events from events store...");
					fromMongoEventsStoreEventList = new LinkedList();
					backgammonEvent = null;
				}
				else if(newUserCreatedEvent.getServiceId() == -2){
					logger.info("Recieved the end read events record, reading events from events store completed...");
					logger.info("Updating SnapshotAPI with collected events data from mongo events store...");
					snapshotAPI.setFromMongoEventsStoreEventList(fromMongoEventsStoreEventList);
					logger.info("SnapshotAPI updated...");
					backgammonEvent = null;
				}
				else{
					backgammonEvent = newUserCreatedEvent;
				}
			}
			else if(clazz.equals("NewUserJoinedLobbyEvent")){
				NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = objectMapper.readValue(record.value(), NewUserJoinedLobbyEvent.class);
				backgammonEvent = newUserJoinedLobbyEvent;
			}
			
			logger.info("saving event from events store into list, event = " +backgammonEvent);
			fromMongoEventsStoreEventList.push(backgammonEvent);
		}
		catch(Exception ex){
			logger.error("Failed to save data into redis...");
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}	
}




	