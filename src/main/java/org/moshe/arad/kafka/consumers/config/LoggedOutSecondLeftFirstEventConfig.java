package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedOutSecondLeftFirstEventConfig extends SimpleConsumerConfig{

	public LoggedOutSecondLeftFirstEventConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.LOGGED_OUT_SECOND_LEFT_FIRST_EVENT_GROUP);
	}
}
