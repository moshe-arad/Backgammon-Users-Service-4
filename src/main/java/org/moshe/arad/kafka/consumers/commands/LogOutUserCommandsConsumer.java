package org.moshe.arad.kafka.consumers.commands;

import java.io.IOException;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.entities.Status;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.LogOutUserCommand;
import org.moshe.arad.kafka.events.LoggedOutEvent;
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
public class LogOutUserCommandsConsumer extends SimpleCommandsConsumer {	
	
	@Autowired
	private UsersRepository usersRepository;
	
	@Autowired
	private ApplicationContext context;
	
	private Logger logger = LoggerFactory.getLogger(LogOutUserCommandsConsumer.class);
	
	private ConsumerToProducerQueue toViewServiceQueue;
	
	public LogOutUserCommandsConsumer() {
	}

	@Override
	public void consumerOperations(ConsumerRecord<String, String> record) {
		
		LogOutUserCommand logOutUserCommand = convertJsonBlobIntoEvent(record.value());
		logger.info("Log Out User Command record recieved, " + record.value());
    	
		logger.info("Checking whether this user already exists in system...");
    	
		try{
			BackgammonUser user = usersRepository.isUserExistsAndReturn((logOutUserCommand.getBackgammonUser()));
			if(user != null){
				
				LoggedOutEvent loggedOutEvent = context.getBean(LoggedOutEvent.class);
				loggedOutEvent.setUuid(logOutUserCommand.getUuid());
				loggedOutEvent.setArrived(new Date());
				user.setStatus(Status.LoggedOut);
				loggedOutEvent.setBackgammonUser(user);
				loggedOutEvent.setClazz("LoggedOutEvent");
				
				toViewServiceQueue.getEventsQueue().put(loggedOutEvent);
			}
			else throw new RuntimeException("User was not found...");
			
			logger.info("Ack Reply was send to front service...");
		}
		catch (Exception e) {
			logger.error("Failed to create new user, please try again...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	private LogOutUserCommand convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, LogOutUserCommand.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	public ConsumerToProducerQueue getToViewServiceQueue() {
		return toViewServiceQueue;
	}

	public void setToViewServiceQueue(ConsumerToProducerQueue toViewServiceQueue) {
		this.toViewServiceQueue = toViewServiceQueue;
	}
}




	