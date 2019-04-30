package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;

public class CmdRemote implements IDBGitCommand {

	private Options opts = new Options();
	
	@Override
	public String getCommandName() {
		return "remote";
	}

	@Override
	public String getParams() {
		return "[command] [<params>...]";
	}

	@Override
	public String getHelperInfo() {
		return "Examples:\n"
				+ "    dbgit remote\n"
				+ "    dbgit remote add rep https://login:password@example.com/rep.git\n"
				+ "    dbgit remote remove rep";
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {

		String[] args = cmdLine.getArgs();
		String command = "";
		String name = "";
		String uri = "";
		
		if (args.length > 0)
			command = args[0];
		
		if (args.length > 1)
			name = args[1];
		
		if (args.length > 2)
			uri = args[2];
		
		DBGit.getInstance().gitRemote(command, name, uri);

	}

}
