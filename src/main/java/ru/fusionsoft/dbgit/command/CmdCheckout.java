package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLine.Builder;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdCheckout implements IDBGitCommand {
	
	private Options opts = new Options();

	public CmdCheckout() {
		opts.addOption("b", false, "create and checkout a new branch");
		opts.addOption("r", false, "Updates database");
	}
	
	@Override
	public String getCommandName() {
		return "checkout";
	}

	@Override
	public String getParams() {
		return "<branch> <commit>";
	}

	@Override
	public String getHelperInfo() {
		return "Example:\n"
				+ "    dbgit checkout master";
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		
		String[] args = cmdLine.getArgs();
		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));
		
		if (args == null || args.length == 0) {
			throw new ExceptionDBGit("Bad command. Please specify branch. ");		
		} else if (args.length == 1) {
			DBGit.getInstance().gitCheckout(args[0], null, cmdLine.hasOption("b"));
		} else if (args.length == 2) {
			DBGit.getInstance().gitCheckout(args[0], args[1], cmdLine.hasOption("b"));
		}		
		
		CmdRestore restoreCommand = new CmdRestore();
		
		Builder builder = new CommandLine.Builder();

		if (cmdLine.hasOption("r")) {
			builder.addOption(new Option("r", false, ""));			
		}
		if (cmdLine.hasOption("v")) {
			builder.addOption(new Option("v", false, ""));			
		}

		restoreCommand.execute(builder.build());

	}

}
