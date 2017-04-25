package org.moshe.arad.kafka.commands;

import java.util.Date;
import java.util.UUID;

public class PullEventsCommand implements Commandable {

	private UUID uuid;
	private Date fromDate;
	private boolean isIgnoreDate;
 
	public PullEventsCommand(UUID uuid, Date fromDate, boolean isIgnoreDate) {
		super();
		this.uuid = uuid;
		this.fromDate = fromDate;
		this.isIgnoreDate = isIgnoreDate;
	}
	
	@Override
	public String toString() {
		return "PullEventsCommand [uuid=" + uuid + ", fromDate=" + fromDate + ", isIgnoreDate=" + isIgnoreDate + "]";
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
}
