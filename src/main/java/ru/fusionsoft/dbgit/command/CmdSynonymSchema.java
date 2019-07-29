package ru.fusionsoft.dbgit.command;

import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;


public class CmdSynonymSchema implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdSynonymSchema() {
		opts.addOption("d", false, getLang().getValue("help", "synonym-d").toString());
		opts.addOption("s", false, getLang().getValue("help", "synonym-s").toString());
	}
	
	public String getCommandName() {
		return "synonym";
	}
	
	public String getParams() {
		return "[synonym] [schema]";
	}
	
	public String getHelperInfo() {
		return getLang().getValue("help", "synonym").toString();
	}
	
	public Options getOptions() {
		return opts;
	}
	
	public void execute(CommandLine cmdLine)  throws Exception {
		SchemaSynonym ss = SchemaSynonym.getInstance();
		
		Boolean isShow = cmdLine.hasOption('s');
		
		if (isShow) {
			ConsoleWriter.printlnGreen(getLang().getValue("general", "status", "synSchema"));
			for (Entry<String, String> el : ss.getMapSchema().entrySet()) {
				ConsoleWriter.println(el.getKey() + " - " + el.getValue());
			}
			return ;
		}
		
		if (cmdLine.getArgs().length == 0) {
			throw new ExceptionDBGit(getLang().getValue("errors", "synonym", "badCommand1"));
		}
		
		Boolean isDelete = cmdLine.hasOption('d');
		
		String synonym = cmdLine.getArgs()[0];
		
		if (isDelete) {
			ss.deleteBySynonym(synonym);
		} else {
			if (cmdLine.getArgs().length < 2) {
				throw new ExceptionDBGit(getLang().getValue("errors", "synonym", "badCommand2"));
			}
			
			String schema = cmdLine.getArgs()[1];
			ss.addSchemaSynonym(schema, synonym);			
		}
		
		ss.saveFile();
		ConsoleWriter.println(getLang().getValue("general", "done"));
	}
}
