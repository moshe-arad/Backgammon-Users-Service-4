package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class OpenByLeftEventConfig extends SimpleConsumerConfig{

	public OpenByLeftEventConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.OPENBY_LEFT_EVENT_GROUP);
	}
}
