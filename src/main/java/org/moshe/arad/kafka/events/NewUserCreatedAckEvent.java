package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

public class NewUserCreatedAckEvent extends BackgammonEvent {

	private boolean isUserCreated;

	public NewUserCreatedAckEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz,
			boolean isUserCreated) {
		super(uuid, serviceId, eventId, arrived, clazz);
		this.isUserCreated = isUserCreated;
	}

	@Override
	public String toString() {
		return "NewUserCreatedAckEvent [isUserCreated=" + isUserCreated + "]";
	}

	public boolean isUserCreated() {
		return isUserCreated;
	}

	public void setUserCreated(boolean isUserCreated) {
		this.isUserCreated = isUserCreated;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (isUserCreated ? 1231 : 1237);
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
		NewUserCreatedAckEvent other = (NewUserCreatedAckEvent) obj;
		if (isUserCreated != other.isUserCreated)
			return false;
		return true;
	}
}
