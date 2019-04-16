package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;

public class CmdPush implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdPush() {
		
	}
	
	@Override
	public String getCommandName() {
		return "push";
	}

	@Override
	public String getParams() {
		return "";
	}

	@Override
	public String getHelperInfo() {
		return "_";
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		String[] args = cmdLine.getArgs();
		
		if (args.length > 0) {
			throw new ExceptionDBGit("Bad command. Number of parameters is not correct!");
		}
		
		DBGit.getInstance().gitPush();
	}

}