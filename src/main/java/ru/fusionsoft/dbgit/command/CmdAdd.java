package ru.fusionsoft.dbgit.command;

import java.sql.Timestamp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.MaskFilter;

public class CmdAdd implements IDBGitCommand {

	private Options opts = new Options();
	
	public CmdAdd() {
		
	}
	
	public String getCommandName() {
		return "add";
	}
	
	public String getParams() {
		return "<file_mask>";
	}
	
	public String getHelperInfo() {
		//return "Command for add database objects to dbgit. \n"
		//		+ "You need to specify db object name as parameter in format just like it shows in status command output.\n"
		//		+ "You can also use masks to add many files by one command";
		
		return "Examples: \n"
				+ "    dbgit add <SCHEME>/TEST_TABLE*\n"
				+ "    dbgit add <SCHEME>/TEST_VIEW.vw";
	}
	
	public Options getOptions() {
		return opts;
	}
	
	public void execute(CommandLine cmdLine)  throws Exception {		
		if (cmdLine.getArgs().length == 0) {
			throw new ExceptionDBGit("Bad command. Not found object to add!");
		}
		
		if (!DBGitIndex.getInctance().isCorrectVersion())
			throw new ExceptionDBGit("Versions of Dbgit (" + DBGitIndex.VERSION + ") and repository(" + DBGitIndex.getInctance().getRepoVersion() + ") are different!");
		
		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));
						
		String nameObj = cmdLine.getArgs()[0];
		MaskFilter maskAdd = new MaskFilter(nameObj);
		
		DBGitIndex index = DBGitIndex.getInctance();
		
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();		
		IMapMetaObject dbObjs = gmdm.loadDBMetaData();	
		
		Integer countSave = 0;
		for (IMetaObject obj : dbObjs.values()) {
			if (maskAdd.match(obj.getName())) {			
				Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());
				ConsoleWriter.detailsPrintLn("Processing object " + obj.getName());
				ConsoleWriter.detailsPrint("Saving to file...", 2);
				obj.saveToFile();
				ConsoleWriter.detailsPrintlnGreen("OK");
								
				ConsoleWriter.detailsPrint("Adding to git...", 2);
				countSave += obj.addToGit();				
				ConsoleWriter.detailsPrintlnGreen("OK");
				
    			Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());
    			Long diff = timestampAfter.getTime() - timestampBefore.getTime();
				ConsoleWriter.detailsPrint("Time..." + diff + " ms", 2);
				ConsoleWriter.detailsPrintLn("");
				
				index.addItem(obj);				
			}
		}

		if (countSave > 0) {
			index.saveDBIndex();
			index.addToGit();
		} else {
			ConsoleWriter.printlnRed("Can't find object \"" + nameObj + "\" in database");
		}
		ConsoleWriter.println("Done!");
	}
}
