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
				"    clone      clone a repository into a new directory\n" +
				"    init       create an empty Git repository or reinitialize an existing one\n" + 
				"    link       establishes connection with database\n" + 
				"    synonym    specifies synonym for db scheme\n" +
				"    remote     let you bind your local repository with remote repository\n" +
				"\n" + 
				"work on the current change\n" + 
				"    status     shows current status of db objects\n" + 
				"    add        adds db objects into the dbgit index\n" + 
				"    reset      reset current HEAD to the specified state" +
				"    rm         removes objects from the dbgit index\n" + 
				"    restore    restores db from the dbgit repository\n" + 
				"    dump       dumps db objects into the dbgit repository\n" + 
				"\n" + 
				"grow, mark and tweak your common history\n" + 
				"    valid      checks if dbgit data files are valid\n" +
				"    checkout   switch branches or restore working tree files\n" + 
				"    commit     makes git commit\n" +
				"    merge      join two or more development histories together\n" +
				"\n" + 
				"collaborate\n" +
				"    pull       fetch from and integrate with another repository or a local branch\n" +
				"    push       update remote refs along with associated objects\n" +
				"\n" +
				"See 'dbgit <command> -h' to read about a specific command");
	}

}
