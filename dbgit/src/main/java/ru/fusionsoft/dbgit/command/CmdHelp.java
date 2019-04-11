package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.diogonunes.jcdp.color.api.Ansi.FColor;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdHelp implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdHelp() {
		
	}
	
	public String getCommandName() {
		return "help";
	}
	
	public String getParams() {
		return "";
	}
	
	public String getHelperInfo() {
		return "Command shows this help";
	}
	
	public Options getOptions() {
		return opts;
	}
	@Override
	public void execute(CommandLine cmdLine) throws Exception {	
		ConsoleWriter.println("usage: dbgit <command> [<args>]\n" + 
				"\n" + 
				"These are common Dbgit commands used in various situations:\n" + 
				"\n" + 
				"start a working area\n" + 
				"    link       establishes connection with database\n" + 
				"    synonym    specifies synonym for db scheme\n" + 
				"\n" + 
				"work on the current change\n" + 
				"    status     shows current status of db objects\n" + 
				"    add        adds db objects into the dbgit index\n" + 
				"    rm         removes objects from the dbgit index\n" + 
				"    restore    restores db from the dbgit repository\n" + 
				"    dump       dumps db objects into the dbgit repository\n" + 
				"\n" + 
				"grow, mark and tweak your common history\n" + 
				"    valid      checks if dbgit data files are valid\n" + 
				"    commit     makes git commit\n" + 
				"\n" + 
				"See 'dbgit <command> -h' to read about a specific command");
	}

}
