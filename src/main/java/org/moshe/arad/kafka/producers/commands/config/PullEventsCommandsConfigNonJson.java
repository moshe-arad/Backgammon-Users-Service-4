package org.moshe.arad.kafka.producers.commands.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.producers.SimpleProducerConfig;
import org.springframework.stereotype.Component;

@Component
public class PullEventsCommandsConfigNonJson extends SimpleProducerConfig{

	public PullEventsCommandsConfigNonJson() {
		super();
		super.getProperties().put("value.serializer", KafkaUtils.PULL_EVENTS_COMMAND_SERIALIZER);
	}
}
