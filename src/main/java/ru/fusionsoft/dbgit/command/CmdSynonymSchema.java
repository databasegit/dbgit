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
		opts.addOption("d", false, "Delete synonym");
		opts.addOption("s", false, "Show synonyms");
	}
	
	public String getCommandName() {
		return "synonym";
	}
	
	public String getParams() {
		return "[synonym] [schema]";
	}
	
	public String getHelperInfo() {
		return "Examples:\n"
			+ "    dbgit synonym <syn> <scheme>\n"
			+ "    dbgit synonym <synonym> -d \n"
			+ "    dbgit synonym -s";
	}
	
	public Options getOptions() {
		return opts;
	}
	
	public void execute(CommandLine cmdLine)  throws Exception {
		SchemaSynonym ss = SchemaSynonym.getInctance();
		
		Boolean isShow = cmdLine.hasOption('s');
		
		if (isShow) {
			ConsoleWriter.printlnGreen("Synonym - schema");
			for (Entry<String, String> el : ss.getMapSchema().entrySet()) {
				ConsoleWriter.println(el.getKey() + " - " + el.getValue());
			}
			return ;
		}
		
		if (cmdLine.getArgs().length == 0) {
			throw new ExceptionDBGit("Bad command. Please specify synonym and scheme. ");
		}
		
		Boolean isDelete = cmdLine.hasOption('d');
		
		String synonym = cmdLine.getArgs()[0];
		
		if (isDelete) {
			ss.deleteBySynonym(synonym);
		} else {
			if (cmdLine.getArgs().length < 2) {
				throw new ExceptionDBGit("Bad command. Please specify scheme. ");
			}
			
			String schema = cmdLine.getArgs()[1];
			ss.addSchemaSynonym(schema, synonym);			
		}
		
		ss.saveFile();
		ConsoleWriter.println("Synonyms save.");
	}
}
