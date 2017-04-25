package org.moshe.arad.kafka.consumers.commands;

import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.CreateNewUserCommand;
import org.moshe.arad.kafka.consumers.SimpleConsumerConfig;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.springframework.stereotype.Component;

@Component
public class CreateNewUserCommandsConsumer extends SimpleBackgammonCommandsConsumer<CreateNewUserCommand> {

	private ConsumerToProducerQueue consumerToProducerQueue;
	
	public CreateNewUserCommandsConsumer() {
	}
	
	public CreateNewUserCommandsConsumer(SimpleConsumerConfig simpleConsumerConfig, String topic) {
		super(simpleConsumerConfig, topic);
	}

	@Override
	public void consumerOperations(ConsumerRecord<String, CreateNewUserCommand> record) {
		logger.info("Create New User Command record recieved, " + record.value());
//    	logger.info("Start to check whether this user already exists in system...");
    	
		logger.info("Creating New User Created Event... ");
		NewUserCreatedEvent newUserCreatedEvent = new NewUserCreatedEvent(record.value().getUuid(), 
				1, 1, new Date(), "NewUserCreatedEvent", record.value().getBackgammonUser()); 
    	consumerToProducerQueue.getEventsQueue().put(newUserCreatedEvent);
    	logger.info("Event created and passed to consumer...");
	}

	public ConsumerToProducerQueue getConsumerToProducerQueue() {
		return consumerToProducerQueue;
	}

	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}	
}




	