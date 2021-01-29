package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdCommit implements IDBGitCommand {

	private Options opts = new Options();
	
	public CmdCommit() {
		opts.addOption("a", false, getLang().getValue("help", "commit-a").toString());
		opts.addOption("m", true, getLang().getValue("help", "commit-m").toString());
	}
	
	@Override
	public String getCommandName() {
		return "commit";
	}

	@Override
	public String getParams() {
		return "<pathspec>";
	}

	@Override
	public String getHelperInfo() {
		return getLang().getValue("help", "commit").toString();
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		String[] args = cmdLine.getArgs();
		
		String filePath = "";
		if(!(args == null || args.length == 0)) {
			filePath = args[0];			
		}		
		
		String msg = "";
		
		if (cmdLine.hasOption("m")) {
			msg = cmdLine.getOptionValue("m");
		}
		
		checkVersion();
		
		ConsoleWriter.println(getLang().getValue("general", "commit", "commiting"), messageLevel);
		DBGitIndex.getInctance().addLinkToGit();
		DBGitIndex.getInctance().addIgnoreToGit();
		DBGit.getInstance().gitCommit(cmdLine.hasOption("a"), msg, filePath);
	}

}
