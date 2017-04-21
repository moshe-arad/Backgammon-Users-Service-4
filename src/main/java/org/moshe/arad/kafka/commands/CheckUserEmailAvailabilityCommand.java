package org.moshe.arad.kafka.commands;

import java.util.UUID;

import org.springframework.stereotype.Component;

@Component("CheckUserEmailAvailabilityCommand")
public class CheckUserEmailAvailabilityCommand implements Commandable {

	private UUID uuid;
	private String email;

	public CheckUserEmailAvailabilityCommand(UUID uuid, String email) {
		this.email = email;
		this.uuid = uuid;
	}
	
	@Override
	public String toString() {
		return "CheckUserEmailAvailabilityCommand [email=" + email + "]";
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	protected UUID getUuid() {
		return uuid;
	}

	protected void setUuid(UUID uuid) {
		this.uuid = uuid;
	}
}
