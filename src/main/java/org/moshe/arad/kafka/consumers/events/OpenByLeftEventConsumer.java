package org.moshe.arad.kafka.consumers.events;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEvent;
import org.moshe.arad.kafka.events.OpenByLeftEvent;
import org.moshe.arad.kafka.events.UserAddedAsSecondPlayerEvent;
import org.moshe.arad.kafka.events.UserAddedAsWatcherEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftBeforeGameStartedEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutUserLeftLobbyEvent;
import org.moshe.arad.kafka.events.UserPermissionsUpdatedEvent;
import org.moshe.arad.repository.UsersRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@Scope("prototype")
public class OpenByLeftEventConsumer extends SimpleEventsConsumer {

	@Autowired
	private UsersRepository usersRepository;
	
	@Autowired
	private ApplicationContext context;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	Logger logger = LoggerFactory.getLogger(OpenByLeftEventConsumer.class);
	
	public OpenByLeftEventConsumer() {
	}
	
	@Override
	public void consumerOperations(ConsumerRecord<String, String> record) {
		OpenByLeftEvent openByLeftEvent = convertJsonBlobIntoEvent(record.value());
		
		try{			
			String leavingUserName = openByLeftEvent.getOpenBy();
			BackgammonUser backgammonUser = context.getBean(BackgammonUser.class);
			backgammonUser.setUserName(leavingUserName);
			backgammonUser = usersRepository.isUserExistsAndReturn(backgammonUser);
			
			if(backgammonUser != null){
				logger.info("User found...");
				logger.info("Will set new permissions to user...");
				UserPermissionsUpdatedEvent userPermissionsUpdatedEvent = context.getBean(UserPermissionsUpdatedEvent.class);
				backgammonUser.setUser_permissions(Arrays.asList("user"));
				userPermissionsUpdatedEvent.setBackgammonUser(backgammonUser);
				
				userPermissionsUpdatedEvent.setUuid(openByLeftEvent.getUuid());
				userPermissionsUpdatedEvent.setArrived(new Date());
				userPermissionsUpdatedEvent.setClazz("UserPermissionsUpdatedEvent");
				
				logger.info("passing User Permissions Updated Event to kafka broker...");
				consumerToProducerQueue.getEventsQueue().put(userPermissionsUpdatedEvent);
				logger.info("Event passed to kafka broker...");
			}
		}
		catch(Exception e){
			logger.error("Failed to add new game room to view...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	private OpenByLeftEvent convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, OpenByLeftEvent.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
	
	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}

}
