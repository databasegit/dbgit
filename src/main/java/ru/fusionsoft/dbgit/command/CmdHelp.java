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
		return getLang().getValue("help", "h").toString();
	}
	
	public Options getOptions() {
		return opts;
	}
	@Override
	public void execute(CommandLine cmdLine) throws Exception {	
		ConsoleWriter.println(getLang().getValue("help", "common"));
	}

}
