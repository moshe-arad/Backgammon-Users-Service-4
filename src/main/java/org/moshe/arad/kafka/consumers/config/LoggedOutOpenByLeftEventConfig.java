package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedOutOpenByLeftEventConfig extends SimpleConsumerConfig{

	public LoggedOutOpenByLeftEventConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.LOGGED_OUT_OPENBY_LEFT_EVENT_GROUP);
	}
}
