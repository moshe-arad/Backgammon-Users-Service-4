package org.moshe.arad.kafka.producers.commands.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.producers.SimpleProducerConfig;
import org.springframework.stereotype.Component;

@Component
public class PullEventsCommandsConfig extends SimpleProducerConfig{

	public PullEventsCommandsConfig() {
		super();
		super.getProperties().put("value.serializer", KafkaUtils.PULL_EVENTS_COMMAND_SERIALIZER);
	}
}
