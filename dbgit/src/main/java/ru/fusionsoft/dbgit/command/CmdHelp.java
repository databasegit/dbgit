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
		ConsoleWriter.println("This help!!!");
		ConsoleWriter.println("For see help about command execute: dbgit command -h");
		ConsoleWriter.println("List commands:");
		
		RequestCmd cmdReq = RequestCmd.getInctance();
		
		for (IDBGitCommand cmd : cmdReq.getCommands().values()) {
			ConsoleWriter.printlnColor(cmd.getCommandName(), FColor.GREEN, 0);
			ConsoleWriter.println(cmd.getHelperInfo(), 1);
		}
	}

}
