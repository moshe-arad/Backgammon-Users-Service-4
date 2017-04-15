package org.moshe.arad.kafka;

import java.util.concurrent.Callable;

import org.moshe.arad.kafka.events.BackgammonEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class ConsumerToProducerJob implements Callable<BackgammonEvent>{

	@Autowired
	private ConsumerToProducerQueue consumerToProducerQueue;
		
	public ConsumerToProducerJob(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}

	@Override
	public BackgammonEvent call() throws Exception {
		while(true){
			return consumerToProducerQueue.getEventsQueue().take();
		}
	}

}
