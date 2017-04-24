package org.moshe.arad.kafka.producers.commands;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.kafka.commands.PullEventsCommand;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PullEventsCommandsProducer extends SimpleBackgammonCommandsProducer<PullEventsCommand>{

	@Autowired
	private SnapshotAPI snapshotAPI;
	
	@Override
	public void doProducerCommandsOperations() {
		Date lastUpdate = snapshotAPI.getLastUpdateSnapshotDate();
		
		PullEventsCommand pullEventsCommand = new PullEventsCommand(UUID.randomUUID(), lastUpdate);
		sendKafkaMessage(pullEventsCommand);		
	}
}
