package org.moshe.arad.kafka.consumers.events.json;

import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.consumers.SimpleConsumerConfig;
import org.springframework.stereotype.Component;

@Component
public class FromMongoEventsStoreEventConfig extends SimpleConsumerConfig {

	public FromMongoEventsStoreEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.FROM_MONGO_TO_USERS_SERVICE_GROUP);
		super.getProperties().put("value.deserializer", KafkaUtils.STRING_DESERIALIZER);
	}
}
