package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

public class FromMongoEventsStoreEvent extends BackgammonEvent{

	private BackgammonEvent backgammonEvent;
	private boolean isStartReadEvents;
	private boolean isEndReadEvents;
	
	public FromMongoEventsStoreEvent() {
	}

	public FromMongoEventsStoreEvent(UUID uuid, int serviceId, String serviceName, int entityId, String entityType,
			int eventId, String eventType, BackgammonEvent backgammonEvent, boolean isStartReadEvents,
			boolean isEndReadEvents) {
		super(uuid, serviceId, serviceName, entityId, entityType, eventId, eventType);
		this.backgammonEvent = backgammonEvent;
		this.isStartReadEvents = isStartReadEvents;
		this.isEndReadEvents = isEndReadEvents;
	}

	public FromMongoEventsStoreEvent(UUID uuid, int serviceId, String serviceName, int entityId, String entityType,
			int eventId, String eventType, Date arrived, BackgammonEvent backgammonEvent, boolean isStartReadEvents,
			boolean isEndReadEvents) {
		super(uuid, serviceId, serviceName, entityId, entityType, eventId, eventType, arrived);
		this.backgammonEvent = backgammonEvent;
		this.isStartReadEvents = isStartReadEvents;
		this.isEndReadEvents = isEndReadEvents;
	}

	@Override
	public String toString() {
		return "FromMongoEventsStoreEvent [backgammonEvent=" + backgammonEvent + ", isStartReadEvents="
				+ isStartReadEvents + ", isEndReadEvents=" + isEndReadEvents + "]";
	}

	public BackgammonEvent getBackgammonEvent() {
		return backgammonEvent;
	}

	public void setBackgammonEvent(BackgammonEvent backgammonEvent) {
		this.backgammonEvent = backgammonEvent;
	}

	public boolean isStartReadEvents() {
		return isStartReadEvents;
	}

	public void setStartReadEvents(boolean isStartReadEvents) {
		this.isStartReadEvents = isStartReadEvents;
	}

	public boolean isEndReadEvents() {
		return isEndReadEvents;
	}

	public void setEndReadEvents(boolean isEndReadEvents) {
		this.isEndReadEvents = isEndReadEvents;
	}
}
