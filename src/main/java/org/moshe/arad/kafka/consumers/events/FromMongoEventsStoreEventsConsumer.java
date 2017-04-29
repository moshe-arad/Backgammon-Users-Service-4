package org.moshe.arad.kafka.consumers.events;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.moshe.arad.kafka.producers.commands.PullEventsCommandsProducer;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@Scope("prototype")
public class FromMongoEventsStoreEventsConsumer extends SimpleEventsConsumer {
	
	@Autowired
	private SnapshotAPI snapshotAPI;
	
	Logger logger = LoggerFactory.getLogger(FromMongoEventsStoreEventsConsumer.class);
	
	private ArrayList<BackgammonEvent> fromMongoEventsStoreEventList; 
	
	private Set<BackgammonEvent> eventsFromMongoSet = new HashSet<>(10000);

	private int totalNumOfEvents;
	private String uuid;
	private boolean isToSaveEvents;
	
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
			
			if(clazz.equals("StartReadEventsFromMongoEvent"))
			{
				logger.info("Recieved the begin read events record, starting reading events from events store...");
				totalNumOfEvents = jsonNode.get("totalNumOfEvents").asInt();
				uuid = jsonNode.get("uuid").asText();
				isToSaveEvents = jsonNode.get("toSaveEvents").asBoolean();
			}
			else if(clazz.equals("EndReadEventsFromMongoEvent")){					
				logger.info("Recieved the end read events record, reading events from events store completed...");
				logger.info("Updating SnapshotAPI with collected events data from mongo events store...");
				
				while(countEventsFromMongoByUuid(uuid) != totalNumOfEvents){
					Thread.sleep(1000);
				}
				
				snapshotAPI.setFromMongoEventsStoreEventList(getSortedListByArrivedDate());
				if(isToSaveEvents){
					snapshotAPI.updateLocalSnapshot();
					logger.info("SnapshotAPI updated...");
					
					Iterator<Object> it = snapshotAPI.getUpdateSnapshotLocker().iterator();
					
					while(it.hasNext()){
						Object locker = it.next();
						synchronized (locker) {
							PullEventsCommandsProducer.setUpdating(false);
							locker.notifyAll();
						}
					}					
				}
				else{
					synchronized (snapshotAPI.getUsersLockers().get(UUID.fromString((uuid)))) {
						snapshotAPI.saveTempSnapshot();
						logger.info("Temporary snapshot created...");
						snapshotAPI.getUsersLockers().get(UUID.fromString((uuid))).notifyAll();						
					}					
					snapshotAPI.getUsersLockers().remove(UUID.fromString((uuid)));
				}								
			}
			else if(clazz.equals("NewUserCreatedEvent")){
				NewUserCreatedEvent newUserCreatedEvent = objectMapper.readValue(record.value(), NewUserCreatedEvent.class);
				backgammonEvent = newUserCreatedEvent;
				eventsFromMongoSet.add(backgammonEvent);
			}
			else if(clazz.equals("NewUserJoinedLobbyEvent")){
				NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = objectMapper.readValue(record.value(), NewUserJoinedLobbyEvent.class);
				backgammonEvent = newUserJoinedLobbyEvent;
				eventsFromMongoSet.add(backgammonEvent);
			}
		}
		catch(Exception ex){
			logger.error("Failed to save data into redis...");
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}

	@Override
	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		
	}
	
	private int countEventsFromMongoByUuid(String uuid){
		int count = 0;
		Iterator<BackgammonEvent> it = eventsFromMongoSet.iterator();
		
		while(it.hasNext()){
			BackgammonEvent event = it.next();
			if(event.getUuid().toString().equals(uuid)) count++;
		}
		
		return count;
	}
	
	public LinkedList<BackgammonEvent> getSortedListByArrivedDate(){
		fromMongoEventsStoreEventList = new ArrayList<>(eventsFromMongoSet);
		fromMongoEventsStoreEventList = (ArrayList<BackgammonEvent>) fromMongoEventsStoreEventList.stream().sorted((BackgammonEvent e1, BackgammonEvent e2) -> {return e1.getArrived().compareTo(e2.getArrived());}).collect(Collectors.toList());
		return new LinkedList<>(fromMongoEventsStoreEventList);
	}	
}




	