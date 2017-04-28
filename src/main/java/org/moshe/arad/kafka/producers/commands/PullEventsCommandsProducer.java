package org.moshe.arad.kafka.producers.commands;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.kafka.commands.PullEventsCommand;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


@Component
@Scope("prototype")
public class PullEventsCommandsProducer extends SimpleCommandsProducer<PullEventsCommand>{

	@Autowired
	private SnapshotAPI snapshotAPI;
	
	private boolean isToSaveEvents;	
	
	@Override
	public void doProducerCommandsOperations(UUID uuid) {
		PullEventsCommand pullEventsCommand;
		Date lastUpdate = snapshotAPI.getLastUpdateSnapshotDate();		
		
		if(lastUpdate == null){
			if(isToSaveEvents) pullEventsCommand = new PullEventsCommand(uuid, new Date(), true, true);
			else pullEventsCommand = new PullEventsCommand(uuid, new Date(), true, false);
		}
		else{			
			if(isToSaveEvents) pullEventsCommand = new PullEventsCommand(uuid, lastUpdate, false, true);
			else pullEventsCommand = new PullEventsCommand(uuid, lastUpdate, false, false);
		}
		snapshotAPI.setLatestSnapshotDate(new Date());
		sendKafkaMessage(pullEventsCommand);		
	}
	
	public boolean isToSaveEvent() {
		return isToSaveEvents;
	}

	@Override
	public void setToSaveEvent(boolean isToSaveEvent) {
		this.isToSaveEvents = isToSaveEvent;
	}
}
