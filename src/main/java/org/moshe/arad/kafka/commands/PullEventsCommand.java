package org.moshe.arad.kafka.commands;

import java.util.Date;
import java.util.UUID;

public class PullEventsCommand implements Commandable {

	private UUID uuid;
	private Date fromDate;
	
	public PullEventsCommand(UUID uuid, Date fromDate) {
		super();
		this.uuid = uuid;
		this.fromDate = fromDate;
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
}
