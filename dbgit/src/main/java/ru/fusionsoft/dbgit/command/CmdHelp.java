package ru.fusionsoft.dbgit.command;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdHelp implements IDBGitCommand {

	public void execute(String[] args)  throws Exception {
		ConsoleWriter.println("This help!!!");
	}

}
