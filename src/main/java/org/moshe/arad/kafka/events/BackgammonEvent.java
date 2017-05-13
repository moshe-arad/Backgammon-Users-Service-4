package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

public abstract class BackgammonEvent implements Comparable<BackgammonEvent>{

	private UUID uuid;
	private int serviceId;
	private int eventId;
	private Date arrived;
	private String clazz;
	
	public BackgammonEvent() {
	}	

	public BackgammonEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz) {
		super();
		this.uuid = uuid;
		this.serviceId = serviceId;
		this.eventId = eventId;
		this.arrived = arrived;
		this.clazz = clazz;
	}

	public int getServiceId() {
		return serviceId;
	}

	public void setServiceId(int serviceId) {
		this.serviceId = serviceId;
	}

	public int getEventId() {
		return eventId;
	}

	public void setEventId(int eventId) {
		this.eventId = eventId;
	}

	public Date getArrived() {
		return arrived;
	}

	public void setArrived(Date arrived) {
		this.arrived = arrived;
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}

	public String getClazz() {
		return clazz;
	}

	public void setClazz(String clazz) {
		this.clazz = clazz;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((arrived == null) ? 0 : arrived.hashCode());
		result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
		result = prime * result + eventId;
		result = prime * result + serviceId;
		result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BackgammonEvent other = (BackgammonEvent) obj;
		if (arrived == null) {
			if (other.arrived != null)
				return false;
		} else if (!arrived.equals(other.arrived))
			return false;
		if (clazz == null) {
			if (other.clazz != null)
				return false;
		} else if (!clazz.equals(other.clazz))
			return false;
		if (eventId != other.eventId)
			return false;
		if (serviceId != other.serviceId)
			return false;
		if (uuid == null) {
			if (other.uuid != null)
				return false;
		} else if (!uuid.equals(other.uuid))
			return false;
		return true;
	}
}
