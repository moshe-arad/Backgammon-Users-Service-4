package org.moshe.arad.kafka.events;

public class StartReadEventsFromMongoEvent extends BackgammonEvent {

	public StartReadEventsFromMongoEvent() {
	}

	@Override
	public String toString() {
		return "StartReadEventsFromMongoEvent []";
	}
	
	@Override
	public int compareTo(BackgammonEvent o) {
		return this.compareTo(o);
	}
}
