package org.moshe.arad.kafka.consumers.events;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEvent;
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
public class NewGameRoomOpenedEventConsumer extends SimpleEventsConsumer {

	@Autowired
	private UsersRepository usersRepository;
	
	@Autowired
	private ApplicationContext context;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	Logger logger = LoggerFactory.getLogger(NewGameRoomOpenedEventConsumer.class);
	
	public NewGameRoomOpenedEventConsumer() {
	}
	
	@Override
	public void consumerOperations(ConsumerRecord<String, String> record) {
		NewGameRoomOpenedEvent newGameRoomOpenedEvent = convertJsonBlobIntoEvent(record.value());
		
		try{
			BackgammonUser findUser = context.getBean(BackgammonUser.class);
			findUser.setUserName(newGameRoomOpenedEvent.getGameRoom().getOpenBy());
			
			BackgammonUser user = usersRepository.isUserExistsAndReturn(findUser);
			
			if(user != null){
				logger.info("User found...");
				logger.info("Will set new permissions to user...");
				UserPermissionsUpdatedEvent userPermissionsUpdatedEvent = context.getBean(UserPermissionsUpdatedEvent.class);
				user.setUser_permissions(Arrays.asList("user", newGameRoomOpenedEvent.getGameRoom().getName()));
				userPermissionsUpdatedEvent.setBackgammonUser(user);
				
				userPermissionsUpdatedEvent.setUuid(newGameRoomOpenedEvent.getUuid());
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
	
	private NewGameRoomOpenedEvent convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, NewGameRoomOpenedEvent.class);
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
