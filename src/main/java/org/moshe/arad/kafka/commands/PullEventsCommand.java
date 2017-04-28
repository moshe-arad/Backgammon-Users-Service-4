package org.moshe.arad.kafka.commands;

import java.util.Date;
import java.util.UUID;

public class PullEventsCommand implements ICommand {

	private UUID uuid;
	private Date fromDate;
	private boolean isIgnoreDate;
	private boolean isToSaveEvents;
 
	public PullEventsCommand(UUID uuid, Date fromDate, boolean isIgnoreDate, boolean isToSaveEvents) {
		super();
		this.uuid = uuid;
		this.fromDate = fromDate;
		this.isIgnoreDate = isIgnoreDate;
		this.isToSaveEvents = isToSaveEvents;
	}
	
	@Override
	public String toString() {
		return "PullEventsCommand [uuid=" + uuid + ", fromDate=" + fromDate + ", isIgnoreDate=" + isIgnoreDate
				+ ", isToSaveEvents=" + isToSaveEvents + "]";
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}

	public Date getFromDate() {
		return fromDate;
	}

	public void setFromDate(Date fromDate) {
		this.fromDate = fromDate;
	}

	public boolean isIgnoreDate() {
		return isIgnoreDate;
	}

	public void setIgnoreDate(boolean isIgnoreDate) {
		this.isIgnoreDate = isIgnoreDate;
	}

	public boolean isToSaveEvents() {
		return isToSaveEvents;
	}

	public void setToSaveEvents(boolean isToSaveEvents) {
		this.isToSaveEvents = isToSaveEvents;
	}
}
