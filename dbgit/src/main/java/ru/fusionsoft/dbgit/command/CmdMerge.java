package ru.fusionsoft.dbgit.command;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;

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
		return "_";
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		String[] args = cmdLine.getArgs();
		
		Set<String> branches = new HashSet<String>();
		
		for (String arg : args)
			branches.add(arg);
		
		DBGit.getInstance().gitMerge(branches);

	}

}
