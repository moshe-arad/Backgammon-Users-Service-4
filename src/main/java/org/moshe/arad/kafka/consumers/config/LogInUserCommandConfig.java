package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LogInUserCommandConfig extends SimpleConsumerConfig{

	public LogInUserCommandConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.LOG_IN_USER_COMMAND_GROUP);
	}
}
