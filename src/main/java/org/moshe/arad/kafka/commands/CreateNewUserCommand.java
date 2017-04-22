package org.moshe.arad.kafka.commands;

import java.util.UUID;

import org.moshe.arad.entities.BackgammonUser;

public class CreateNewUserCommand implements Commandable {

	private UUID uuid;
	private BackgammonUser backgammonUser;
	
	public CreateNewUserCommand() {
	}

	public CreateNewUserCommand(UUID uuid, BackgammonUser backgammonUser) {
		super();
		this.uuid = uuid;
		this.backgammonUser = backgammonUser;
	}

	@Override
	public String toString() {
		return "CreateNewUserCommand [uuid=" + uuid + ", backgammonUser=" + backgammonUser + "]";
	}

	public BackgammonUser getBackgammonUser() {
		return backgammonUser;
	}

	public void setBackgammonUser(BackgammonUser backgammonUser) {
		this.backgammonUser = backgammonUser;
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}
}
