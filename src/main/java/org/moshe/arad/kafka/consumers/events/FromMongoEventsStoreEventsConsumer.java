package org.moshe.arad.kafka.consumers.events;

import java.util.LinkedList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.kafka.consumers.SimpleConsumerConfig;
import org.moshe.arad.kafka.events.FromMongoEventsStoreEvent;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FromMongoEventsStoreEventsConsumer extends SimpleBackgammonEventsConsumer<FromMongoEventsStoreEvent> {
	
	@Autowired
	private SnapshotAPI snapshotAPI;
	
	Logger logger = LoggerFactory.getLogger(FromMongoEventsStoreEventsConsumer.class);
	
	private boolean isReadingEvents = false;
	
	private LinkedList<FromMongoEventsStoreEvent> fromMongoEventsStoreEventList = new LinkedList();
	
	public FromMongoEventsStoreEventsConsumer() {
	}
	
	public FromMongoEventsStoreEventsConsumer(SimpleConsumerConfig simpleConsumerConfig, String topic) {
		super(simpleConsumerConfig, topic);
	}

	@Override
	public void consumerOperations(ConsumerRecord<String,FromMongoEventsStoreEvent> record) {
		try{
			logger.info("From Mongo Events Store Event record recieved, " + record.value());
			logger.info("Checking if can start read events from events store...");
			
			if(record.value().isEndReadEvents() == true){
				isReadingEvents = false;
				logger.info("Updating SnapshotAPI with collected events data from mongo events store...");
				snapshotAPI.setFromMongoEventsStoreEventList(fromMongoEventsStoreEventList);
				logger.info("SnapshotAPI updated...");
			}
			else if(isReadingEvents == true){
				logger.info("Check passed, read events from Mongo events store...");
				fromMongoEventsStoreEventList.push(record.value());
				logger.info("Event saved on list, event = " + record.value());
			}
			else if(record.value().isStartReadEvents() == true) isReadingEvents = true;
		}
		catch(Exception ex){
			logger.error("Failed to save data into redis...");
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}	
}




	