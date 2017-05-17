package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.entities.BackgammonUser;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class LogOutUserAckEvent extends BackgammonEvent {

	private boolean isUserFound;
	private BackgammonUser backgammonUser;
	
	public LogOutUserAckEvent() {
	
	}

	public LogOutUserAckEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz, boolean isUserFound,
			BackgammonUser backgammonUser) {
		super(uuid, serviceId, eventId, arrived, clazz);
		this.isUserFound = isUserFound;
		this.backgammonUser = backgammonUser;
	}


	public LogOutUserAckEvent(boolean isUserFound, BackgammonUser backgammonUser) {
		super();
		this.isUserFound = isUserFound;
		this.backgammonUser = backgammonUser;
	}

	@Override
	public String toString() {
		return "LogOutUserAckEvent [isUserFound=" + isUserFound + ", backgammonUser=" + backgammonUser + "]";
	}

	public boolean isUserFound() {
		return isUserFound;
	}

	public void setUserFound(boolean isUserFound) {
		this.isUserFound = isUserFound;
	}

	public BackgammonUser getBackgammonUser() {
		return backgammonUser;
	}

	public void setBackgammonUser(BackgammonUser backgammonUser) {
		this.backgammonUser = backgammonUser;
	}	
}
