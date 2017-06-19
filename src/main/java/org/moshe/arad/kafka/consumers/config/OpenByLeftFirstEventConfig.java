package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class OpenByLeftFirstEventConfig extends SimpleConsumerConfig{

	public OpenByLeftFirstEventConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.OPENBY_LEFT_FIRST_EVENT_GROUP);
	}
}
