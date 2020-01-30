package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ru.fusionsoft.dbgit.core.DBGit;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CmdMerge implements IDBGitCommand {
	
	private Options opts = new Options();

	@Override
	public String getCommandName() {
		return "merge";
	}

	@Override
	public String getParams() {
		return "[<commit>...]";
	}

	@Override
	public String getHelperInfo() {
		return getLang().getValue("help", "merge").toString();
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		String[] args = cmdLine.getArgs();

		Set<String> branches = new HashSet<>(Arrays.asList(args));
		
		DBGit.getInstance().gitMerge(branches);

	}

}
