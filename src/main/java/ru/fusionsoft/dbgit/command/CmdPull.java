package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;

public class CmdPull implements IDBGitCommand {

	private Options opts = new Options();
	
	public CmdPull() {
		
	}
	
	@Override
	public String getCommandName() {
		return "pull";
	}

	@Override
	public String getParams() {
		return "[<repository> [<refspec>...]]";
	}

	@Override
	public String getHelperInfo() {
		return "Example:\n"
				+ "    dbgit pull origin master";
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		String[] args = cmdLine.getArgs();
		String remote = "";
		String remoteBranch = "";
		
		if (args.length == 1) {
			remote = args[0];
		} else if (args.length == 2) {
			remote = args[0];
			remoteBranch = args[1];
		} else if (args.length > 2) {
			throw new ExceptionDBGit("Bad command. Number of parameters is not correct!");
		}
		
		DBGit.getInstance().gitPull(remote, remoteBranch);
	}

}
