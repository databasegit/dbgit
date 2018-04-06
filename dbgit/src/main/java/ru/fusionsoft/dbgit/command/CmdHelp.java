package ru.fusionsoft.dbgit.command;

public class CmdHelp implements IDBGitCommand {

	public void execute(String[] args)  throws Exception {
		System.out.println("This help!!!");
	}

}
