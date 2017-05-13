package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

public class FromMongoEventsStoreEvent extends BackgammonEvent{

	private BackgammonEvent backgammonEvent;
	private boolean isStartReadEvents;
	private boolean isEndReadEvents;
	
	public FromMongoEventsStoreEvent() {
	}

	public FromMongoEventsStoreEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz,
			BackgammonEvent backgammonEvent, boolean isStartReadEvents, boolean isEndReadEvents) {
		super(uuid, serviceId, eventId, arrived, clazz);
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((backgammonEvent == null) ? 0 : backgammonEvent.hashCode());
		result = prime * result + (isEndReadEvents ? 1231 : 1237);
		result = prime * result + (isStartReadEvents ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		FromMongoEventsStoreEvent other = (FromMongoEventsStoreEvent) obj;
		if (backgammonEvent == null) {
			if (other.backgammonEvent != null)
				return false;
		} else if (!backgammonEvent.equals(other.backgammonEvent))
			return false;
		if (isEndReadEvents != other.isEndReadEvents)
			return false;
		if (isStartReadEvents != other.isStartReadEvents)
			return false;
		return true;
	}
	
	@Override
	public int compareTo(BackgammonEvent o) {
		return this.compareTo(o);
	}
}
