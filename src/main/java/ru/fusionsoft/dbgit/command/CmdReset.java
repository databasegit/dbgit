package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdReset implements IDBGitCommand {

	private Options opts = new Options();
	
	public CmdReset() {
		opts.addOption("soft", false, "");
		opts.addOption("mixed", false, "");
		opts.addOption("hard", false, "");
		opts.addOption("merge", false, "");
		opts.addOption("keep", false, "");
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
		return "Examples: \n"
				+ "    dbgit reset\n"
				+ "    dbgit reset -hard";
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
