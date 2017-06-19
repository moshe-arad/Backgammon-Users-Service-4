package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedOutOpenByLeftFirstEventConfig extends SimpleConsumerConfig{

	public LoggedOutOpenByLeftFirstEventConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.LOGGED_OUT_OPENBY_LEFT_FIRST_EVENT_GROUP);
	}
}
