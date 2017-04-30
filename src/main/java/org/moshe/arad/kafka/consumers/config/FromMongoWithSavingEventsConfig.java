package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class FromMongoWithSavingEventsConfig extends SimpleConsumerConfig {

	public FromMongoWithSavingEventsConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.FROM_MONGO_EVENTS_WITH_SAVING_GROUP);
	}
}
