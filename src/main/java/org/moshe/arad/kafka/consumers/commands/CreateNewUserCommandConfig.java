package org.moshe.arad.kafka.consumers.commands;

import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.consumers.SimpleConsumerConfig;
import org.springframework.stereotype.Component;

@Component("CreateNewUserCommandConfig")
public class CreateNewUserCommandConfig extends SimpleConsumerConfig{

	public CreateNewUserCommandConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.CREATE_NEW_USER_COMMAND_GROUP);
	}
}
