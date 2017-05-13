package org.moshe.arad.kafka.events;

public class EndReadEventsFromMongoEvent extends BackgammonEvent {

	public EndReadEventsFromMongoEvent() {
	}

	@Override
	public String toString() {
		return "EndReadEventsFromMongoEvent []";
	}

	@Override
	public int compareTo(BackgammonEvent o) {
		return this.compareTo(o);
	}
	
	
}
