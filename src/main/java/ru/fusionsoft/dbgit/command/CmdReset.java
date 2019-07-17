package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdReset implements IDBGitCommand {

	private Options opts = new Options();
	
	public CmdReset() {
		opts.addOption("soft", false, getLang().getValue("help", "reset-soft").toString());
		opts.addOption("mixed", false, getLang().getValue("help", "reset-mixed").toString());
		opts.addOption("hard", false, getLang().getValue("help", "reset-hard").toString());
		opts.addOption("merge", false, getLang().getValue("help", "reset-merge").toString());
		opts.addOption("keep", false, getLang().getValue("help", "reset-keep").toString());
	}
	
	@Override
	public String getCommandName() {
		return "reset";
	}

	@Override
	public String getParams() {
		return "[-soft | -mixed [-N] | -hard | -merge | -keep]";
	}

	@Override
	public String getHelperInfo() {
		return getLang().getValue("help", "reset").toString();
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		DBGit.getInstance().gitReset(cmdLine.getOptions().length == 1 ? cmdLine.getOptions()[0].getOpt().toUpperCase() : null);

	}

}
