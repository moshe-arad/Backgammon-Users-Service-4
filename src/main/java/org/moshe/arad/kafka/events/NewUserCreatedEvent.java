package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.entities.BackgammonUser;

public class NewUserCreatedEvent extends BackgammonEvent {

	BackgammonUser backgammonUser;

	public NewUserCreatedEvent() {
	}

	public NewUserCreatedEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz,
			BackgammonUser backgammonUser) {
		super(uuid, serviceId, eventId, arrived, clazz);
		this.backgammonUser = backgammonUser;
	}

	@Override
	public String toString() {
		return "NewUserCreatedEvent [backgammonUser=" + backgammonUser + "]";
	}

	public BackgammonUser getBackgammonUser() {
		return backgammonUser;
	}

	public void setBackgammonUser(BackgammonUser backgammonUser) {
		this.backgammonUser = backgammonUser;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((backgammonUser == null) ? 0 : backgammonUser.hashCode());
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
		NewUserCreatedEvent other = (NewUserCreatedEvent) obj;
		if (backgammonUser == null) {
			if (other.backgammonUser != null)
				return false;
		} else if (!backgammonUser.equals(other.backgammonUser))
			return false;
		return true;
	}
}
