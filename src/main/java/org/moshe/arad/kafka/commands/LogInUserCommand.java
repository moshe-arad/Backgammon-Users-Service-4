package org.moshe.arad.kafka.commands;

import java.util.UUID;

import org.moshe.arad.entities.BackgammonUser;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class LogInUserCommand extends Command {

	private BackgammonUser user;

	public LogInUserCommand() {
	
	}
	
	public LogInUserCommand(UUID uuid, BackgammonUser user) {
		super(uuid);
		this.user = user;
	}

	public LogInUserCommand(BackgammonUser user) {
		super();
		this.user = user;
	}

	@Override
	public String toString() {
		return "LogInUserCommand [user=" + user + "]";
	}

	public BackgammonUser getUser() {
		return user;
	}

	public void setUser(BackgammonUser user) {
		this.user = user;
	}
}
