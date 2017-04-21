package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component("CheckUserNameAvailabilityCommandConfig")
public class CheckUserNameAvailabilityCommandConfig extends SimpleConsumerConfig{

	public CheckUserNameAvailabilityCommandConfig() {
		super();
		super.getProperties().put("value.serializer", KafkaUtils.CHECK_USER_NAME_AVAILABILITY_COMMANDS_DESERIALIZER);
		super.getProperties().put("group.id", KafkaUtils.CHECK_USER_NAME_AVAILABILITY_GROUP);
	}
}
