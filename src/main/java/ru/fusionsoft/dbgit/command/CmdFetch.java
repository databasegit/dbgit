package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdFetch implements IDBGitCommand {

	private Options opts = new Options();
	
	@Override
	public String getCommandName() {
		return "fetch";
	}

	@Override
	public String getParams() {
		return "";
	}

	@Override
	public String getHelperInfo() {
		return getLang().getValue("help", "fetch").toString();
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		
		String[] args = cmdLine.getArgs();
		String remote = "";
		
		ConsoleWriter.println(getLang().getValue("general", "fetch", "fetching"), messageLevel);
		
		if (args.length == 1) {
			remote = args[0];
		} else if (args.length > 1) {
			throw new ExceptionDBGit(getLang().getValue("errors", "paramsNumberIncorrect"));
		}
		
		DBGit.getInstance().gitFetch(remote);

	}

}
