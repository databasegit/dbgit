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
		return "<remote_name>";
	}

	@Override
	public String getHelperInfo() {
		return getLang().getValue("help", "push").toString();
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		String[] args = cmdLine.getArgs();
		
		String remote = "";
	
		if (args.length > 0) {
			remote = args[0];
		}
		
		if (args.length > 1) {
			throw new ExceptionDBGit(getLang().getValue("errors", "paramsNumberIncorrect"));
		}

		DBGit.getInstance().gitPush(remote);
	}

}