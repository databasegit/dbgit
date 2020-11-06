package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;

import java.io.File;

public class CmdClone implements IDBGitCommand {

	private Options opts = new Options();

	public CmdClone() {
		opts.addOption("directory", false, "subdirectory to clone into"/*getLang().getValue("help", "commit-a").toString()*/);
	}

	@Override
	public String getCommandName() {
		return "clone";
	}

	@Override
	public String getParams() {
		return "[link] <remote_name>";
	}

	@Override
	public String getHelperInfo() {
		return getLang().getValue("help", "clone").toString();
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		String[] args = cmdLine.getArgs();
		
		String link = "";
		String remote = "";
		File directory = cmdLine.hasOption("directory") ? new File(cmdLine.getOptionValue("directory")) : null;
		if (args.length > 2) {
			throw new ExceptionDBGit(getLang().getValue("errors", "paramsNumberIncorrect"));
		} else if (args.length == 1) {
			link = args[0];
		} else if (args.length == 2) {
			link = args[0];
			remote = args[1];
		}
		
		DBGit.gitClone(link, remote, directory);

	}

}
