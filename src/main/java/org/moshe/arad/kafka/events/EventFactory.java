package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.BackgammonUser;

public class EventFactory {
	
	public static BackgammonEvent getEvent(Events event, BackgammonUser backgammonUser){
		if(event.equals(Events.NewUserCreatedEvent)){
			return new NewUserCreatedEvent(1, "UsersService", 1, "Users", 1000, Events.NewUserCreatedEvent.name(), backgammonUser);
		}
		else return null;
	}
}
