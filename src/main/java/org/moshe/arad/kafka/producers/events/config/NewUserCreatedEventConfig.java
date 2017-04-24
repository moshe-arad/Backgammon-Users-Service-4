package org.moshe.arad.kafka.producers.events.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.producers.SimpleProducerConfig;
import org.springframework.stereotype.Component;

@Component
public class NewUserCreatedEventConfig extends SimpleProducerConfig {

	public NewUserCreatedEventConfig() {
		super();
		super.getProperties().put("value.serializer", KafkaUtils.NEW_USER_CREATED_EVENT_SERIALIZER);
	}
}
