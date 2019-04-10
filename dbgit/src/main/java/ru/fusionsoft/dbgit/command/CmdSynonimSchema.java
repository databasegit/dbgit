package ru.fusionsoft.dbgit.command;

import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.SchemaSynonim;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;


public class CmdSynonimSchema implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdSynonimSchema() {
		opts.addOption("d", false, "Delete synonim");
		opts.addOption("s", false, "Show synonims");
	}
	
	public String getCommandName() {
		return "synonim";
	}
	
	public String getParams() {
		return "[synonim] [schema]";
	}
	
	public String getHelperInfo() {
		return "Command for create synonim for database schema \n"
				+ "    ways to use: \n"
				+ "        dbgit synonim <scheme1> <scheme2>   - this command creates synonym named <scheme2> for scheme named <scheme1> \n"
				+ "        dbgit synonim <scheme> -d           - this command deletes synonyms of <scheme> \n"
				+ "        dbgit synonim -s                    - shows existing synonyms"
				;
	}
	
	public Options getOptions() {
		return opts;
	}
	
	public void execute(CommandLine cmdLine)  throws Exception {
		SchemaSynonim ss = SchemaSynonim.getInctance();
		
		Boolean isShow = cmdLine.hasOption('s');
		
		if (isShow) {
			ConsoleWriter.printlnGreen("Synonim - schema");
			for (Entry<String, String> el : ss.getMapSchema().entrySet()) {
				ConsoleWriter.println(el.getKey() + " - " + el.getValue());
			}
			return ;
		}
		
		if (cmdLine.getArgs().length == 0) {
			throw new ExceptionDBGit("Bad command. Please specify synonim and scheme. ");
		}
		
		Boolean isDelete = cmdLine.hasOption('d');
		
		String synonim = cmdLine.getArgs()[0];
		
		if (isDelete) {
			ss.deleteBySynonim(synonim);
		} else {
			if (cmdLine.getArgs().length < 2) {
				throw new ExceptionDBGit("Bad command. Please specify scheme. ");
			}
			
			String schema = cmdLine.getArgs()[1];
			ss.addSchemaSynonim(schema, synonim);			
		}
		
		ss.saveFile();
		ConsoleWriter.println("Synonims save.");
	}
}
