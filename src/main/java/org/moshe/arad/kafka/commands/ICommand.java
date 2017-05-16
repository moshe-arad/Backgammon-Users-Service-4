package org.moshe.arad.kafka.commands;

import java.util.UUID;

//TODO need to add system command
public interface ICommand {
	
	public UUID getUuid();
	
	public void setUuid(UUID uuid);
}
