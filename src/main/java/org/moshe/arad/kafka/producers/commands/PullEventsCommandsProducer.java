package org.moshe.arad.kafka.producers.commands;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.kafka.commands.PullEventCommand;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


@Component
@Scope("prototype")
public class PullEventsCommandsProducer extends SimpleCommandsProducer<PullEventCommand>{

	@Autowired
	private SnapshotAPI snapshotAPI;
	
	private boolean isToSaveEvents;	
	
	private static boolean isUpdating = false;
	
	@Override
	public void doProducerCommandsOperations(UUID uuid) {
		PullEventCommand pullEventCommand;
		Date lastUpdate = snapshotAPI.getLastUpdateSnapshotDate();				
		
		if(isToSaveEvents) {			
			if(lastUpdate == null) pullEventCommand = new PullEventCommand(uuid, new Date(), true, true);
			else pullEventCommand = new PullEventCommand(uuid, lastUpdate, false, true);
			snapshotAPI.setLatestSnapshotDate(new Date());
			isUpdating = true;
			sendKafkaMessage(pullEventCommand);
		}
		else{
			if(isUpdating){
				synchronized (this) {
					try {
						this.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}				
			}
			if(lastUpdate == null) pullEventCommand = new PullEventCommand(uuid, new Date(), true, false);
			else pullEventCommand = new PullEventCommand(uuid, lastUpdate, false, false);
			sendKafkaMessage(pullEventCommand);			
		}
	}
	
	public boolean isToSaveEvent() {
		return isToSaveEvents;
	}

	@Override
	public void setToSaveEvent(boolean isToSaveEvent) {
		this.isToSaveEvents = isToSaveEvent;
	}

	public static boolean isUpdating() {
		return isUpdating;
	}

	public static void setUpdating(boolean isUpdating) {
		PullEventsCommandsProducer.isUpdating = isUpdating;
	}

	public SnapshotAPI getSnapshotAPI() {
		return snapshotAPI;
	}
}



//if(lastUpdate == null){
//if(isToSaveEvents) {
//	//TODO need to lock while updating
//	pullEventCommand = new PullEventCommand(uuid, new Date(), true, true);
//}
//else pullEventCommand = new PullEventCommand(uuid, new Date(), true, false);
//}
//else{			
//if(isToSaveEvents){
//	//TODO need to lock while updating
//	pullEventCommand = new PullEventCommand(uuid, lastUpdate, false, true);
//}
//else pullEventCommand = new PullEventCommand(uuid, lastUpdate, false, false);
//}
//
//snapshotAPI.setLatestSnapshotDate(new Date());
//sendKafkaMessage(pullEventCommand);	