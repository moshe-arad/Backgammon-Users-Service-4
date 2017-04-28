package org.moshe.arad.kafka.consumers.commands;

import java.io.IOException;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.kafka.commands.CreateNewUserCommand;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.repository.UsersRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@Scope("prototype")
public class CreateNewUserCommandsConsumer extends SimpleCommandsConsumer {	
	
	@Autowired
	private UsersRepository usersRepository;
	
	private Logger logger = LoggerFactory.getLogger(CreateNewUserCommandsConsumer.class);
	
	public CreateNewUserCommandsConsumer() {
	}

	@Override
	public void consumerOperations(ConsumerRecord<String, String> record) {
		CreateNewUserCommand createNewUserCommand = convertJsonBlobIntoEvent(record.value());
		logger.info("Create New User Command record recieved, " + record.value());
    	
		logger.info("Checking whether this user already exists in system...");
    	
		try{
			if(usersRepository.isUserExists(createNewUserCommand.getBackgammonUser())){
				logger.error("*************************************");
				logger.error("*************************************");
				logger.error("User already exists in system....");
				logger.error("*************************************");
				logger.error("*************************************");
			}
			else{
				logger.info("Creating New User Created Event... ");
				NewUserCreatedEvent newUserCreatedEvent = new NewUserCreatedEvent(createNewUserCommand.getUuid(), 
						1, 1, new Date(), "NewUserCreatedEvent", createNewUserCommand.getBackgammonUser()); 
		    	getConsumerToProducerQueue().getEventsQueue().put(newUserCreatedEvent);
		    	logger.info("Event created and passed to consumer...");
			}
		}
		catch (Exception e) {
			logger.error("Failed to create new user, please try again...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	private CreateNewUserCommand convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, CreateNewUserCommand.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
}




	