package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.entities.BackgammonUser;

public class LogoutUserEvent extends BackgammonEvent {

	BackgammonUser backgammonUser;

	public LogoutUserEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz,
			BackgammonUser backgammonUser) {
		super(uuid, serviceId, eventId, arrived, clazz);
		this.backgammonUser = backgammonUser;
	}

	public LogoutUserEvent(BackgammonUser backgammonUser) {
		super();
		this.backgammonUser = backgammonUser;
	}


	@Override
	public String toString() {
		return "LogoutUserEvent [backgammonUser=" + backgammonUser + "]";
	}

	public BackgammonUser getBackgammonUser() {
		return backgammonUser;
	}

	public void setBackgammonUser(BackgammonUser backgammonUser) {
		this.backgammonUser = backgammonUser;
	}
}
