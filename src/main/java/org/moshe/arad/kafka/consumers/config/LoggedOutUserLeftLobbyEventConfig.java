package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedOutUserLeftLobbyEventConfig extends SimpleConsumerConfig{

	public LoggedOutUserLeftLobbyEventConfig() {
		super();		
		super.getProperties().put("group.id", KafkaUtils.LOGGED_OUT_USER_LEFT_LOBBY_EVENT_GROUP);
	}
}
