package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;

public class CmdInit implements IDBGitCommand {

	private Options opts = new Options();
	
	@Override
	public String getCommandName() {
		return "init";
	}

	@Override
	public String getParams() {
		return "";
	}

	@Override
	public String getHelperInfo() {
		return getLang().getValue("help", "init").toString();
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		String[] args = cmdLine.getArgs();
		
		String dir = "";
		if (args.length > 1) {
			throw new ExceptionDBGit(getLang().getValue("errors", "paramsNumberIncorrect"));
		} else if (args.length == 1) {
			dir = args[0];
		}
		
		DBGit.gitInit(dir);

	}

}
