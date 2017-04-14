package org.moshe.arad.kafka.commands;

import org.moshe.arad.entities.BackgammonUser;

public class CreateNewUserCommand {

	private BackgammonUser backgammonUser;
	
	public CreateNewUserCommand() {
	}

	public CreateNewUserCommand(BackgammonUser backgammonUser) {
		super();
		this.backgammonUser = backgammonUser;
	}

	public BackgammonUser getBackgammonUser() {
		return backgammonUser;
	}

	public void setBackgammonUser(BackgammonUser backgammonUser) {
		this.backgammonUser = backgammonUser;
	}
}
