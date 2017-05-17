package org.moshe.arad.kafka.commands;

import org.moshe.arad.entities.BackgammonUser;
import org.springframework.stereotype.Component;

@Component
public class LogOutUserCommand extends Command {

	private BackgammonUser backgammonUser;

	public LogOutUserCommand() {
	
	}
	
	public LogOutUserCommand(BackgammonUser backgammonUser) {
		super();
		this.backgammonUser = backgammonUser;
	}

	@Override
	public String toString() {
		return "LogOutUserCommand [backgammonUser=" + backgammonUser + "]";
	}

	public BackgammonUser getBackgammonUser() {
		return backgammonUser;
	}

	public void setBackgammonUser(BackgammonUser backgammonUser) {
		this.backgammonUser = backgammonUser;
	}
}
