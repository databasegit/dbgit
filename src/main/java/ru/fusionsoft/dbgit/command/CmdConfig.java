package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdConfig implements IDBGitCommand {
	
	private Options opts = new Options();
	
	public CmdConfig() {
		opts.addOption("g", false, getLang().getValue("help", "config-g").toString());
	}

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
		return getLang().getValue("help", "config").toString();
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		String[] args = cmdLine.getArgs();
		
		if (args.length != 1) {
			throw new ExceptionDBGit(getLang().getValue("errors", "config", "badCommand"));
		} else {
			
			String[] expression = args[0].split("=");
			
			if (cmdLine.hasOption("g")) {
				DBGitConfig.getInstance().setValueGlobal(expression[0], expression[1]);
			} else {
				DBGitConfig.getInstance().setValue(expression[0], expression[1]);
			}
		}

	}

}
