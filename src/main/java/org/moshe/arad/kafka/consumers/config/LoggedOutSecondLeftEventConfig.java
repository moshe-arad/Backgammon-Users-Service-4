package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedOutSecondLeftEventConfig extends SimpleConsumerConfig{

	public LoggedOutSecondLeftEventConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.LOGGED_OUT_SECOND_LEFT_EVENT_GROUP);
	}
}
