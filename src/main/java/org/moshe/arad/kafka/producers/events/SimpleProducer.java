package org.moshe.arad.kafka.producers.events;

import org.moshe.arad.kafka.events.BackgammonEvent;

public interface SimpleProducer {

	public void sendKafkaMessage(BackgammonEvent event);
}
