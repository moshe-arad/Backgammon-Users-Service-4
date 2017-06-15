package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class SecondLeftEventConfig extends SimpleConsumerConfig{

	public SecondLeftEventConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.SECOND_LEFT_EVENT_GROUP);
	}
}
