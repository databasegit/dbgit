package ru.fusionsoft.dbgit.command;

import java.util.HashMap;

public class CommandMap extends HashMap<String, IDBGitCommand> {

	private static final long serialVersionUID = 3806322035854068243L;
	
	public IDBGitCommand put(IDBGitCommand cmd) {
		return put(cmd.getCommandName(), cmd);
	}
	
}
