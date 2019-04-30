package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdConfig implements IDBGitCommand {
	
	private Options opts = new Options();

	@Override
	public String getCommandName() {
		return "config";
	}

	@Override
	public String getParams() {
		return "";
	}

	@Override
	public String getHelperInfo() {
		return "Example:\n"
				+ "    dbgit config LIMIT_FETCH = true";
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		String[] args = cmdLine.getArgs();
		
		if (args.length != 1) {
			throw new ExceptionDBGit("Please specify one parameter to change");
		} else {
			
			String[] expression = args[0].split("=");
			
			DBGitConfig.getInstance().setValue(expression[0], expression[1]);
			
		}

	}

}
