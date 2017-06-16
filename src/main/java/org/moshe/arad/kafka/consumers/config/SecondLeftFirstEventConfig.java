package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class SecondLeftFirstEventConfig extends SimpleConsumerConfig{

	public SecondLeftFirstEventConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.SECOND_LEFT_FIRST_EVENT_GROUP);
	}
}
