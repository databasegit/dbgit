package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;

public class CmdCheckout implements IDBGitCommand {
	
	private Options opts = new Options();

	public CmdCheckout() {
		opts.addOption("b", true, "create and checkout a new branch");
	}
	
	@Override
	public String getCommandName() {
		return "checkout";
	}

	@Override
	public String getParams() {
		return "<branch>";
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
		
		if (cmdLine.hasOption("b")) {
			DBGit.getInstance().gitCheckout(cmdLine.getOptionValue("b"), true);
		} else if (args == null || args.length == 0) {
			throw new ExceptionDBGit("Bad command. Please specify branch. ");		
		} else if (args.length == 1) {
			DBGit.getInstance().gitCheckout(args[0], false);
		}
		
		
		CmdRestore restoreCommand = new CmdRestore();
		restoreCommand.execute(new CommandLine.Builder().build());

	}

}
