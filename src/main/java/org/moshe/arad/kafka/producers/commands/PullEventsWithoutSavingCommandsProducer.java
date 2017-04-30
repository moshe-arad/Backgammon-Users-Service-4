package org.moshe.arad.kafka.producers.commands;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.kafka.commands.PullEventsWithoutSavingCommand;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public class PullEventsWithoutSavingCommandsProducer extends SimpleCommandsProducer<PullEventsWithoutSavingCommand> {

	@Autowired
	private SnapshotAPI snapshotAPI;

	@Autowired
	private ApplicationContext context;
	
	@Override
	public void doProducerCommandsOperations(UUID uuid) {
		PullEventsWithoutSavingCommand pullEventsWithoutSavingCommand = context.getBean(PullEventsWithoutSavingCommand.class);
		Date lastUpdate = snapshotAPI.getLastUpdateDate();	
		
		if(lastUpdate == null){
			pullEventsWithoutSavingCommand.setUuid(uuid);
			pullEventsWithoutSavingCommand.setFromDate(new Date());
			pullEventsWithoutSavingCommand.setIgnoreDate(true);
			
		}
		else{
			pullEventsWithoutSavingCommand.setUuid(uuid);
			pullEventsWithoutSavingCommand.setFromDate(lastUpdate);
			pullEventsWithoutSavingCommand.setIgnoreDate(false);
		}
		
		sendKafkaMessage(pullEventsWithoutSavingCommand);
	}
	
}
