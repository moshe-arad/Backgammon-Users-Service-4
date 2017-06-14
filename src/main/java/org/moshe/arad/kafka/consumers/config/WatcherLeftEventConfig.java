package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class WatcherLeftEventConfig extends SimpleConsumerConfig{

	public WatcherLeftEventConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.WATCHER_LEFT_EVENT_GROUP);
	}
}
