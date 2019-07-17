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
			return getLang().getValue("help", "add").toString();
	}
	
	public Options getOptions() {
		return opts;
	}
	
	public void execute(CommandLine cmdLine)  throws Exception {			
		if (cmdLine.getArgs().length == 0) {
			throw new ExceptionDBGit(getLang().getValue("errors", "add", "badCommand"));
		}
		
		checkVersion();
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
				ConsoleWriter.detailsPrintLn(getLang().getValue("general", "add", "processingObject") + " " + obj.getName());
				ConsoleWriter.detailsPrint(getLang().getValue("general", "add", "savingToFile"), 2);
				obj.saveToFile();
				ConsoleWriter.detailsPrintlnGreen(getLang().getValue("general", "ok"));
								
				ConsoleWriter.detailsPrint(getLang().getValue("general", "addToGit"), 2);
				countSave += obj.addToGit();				
				ConsoleWriter.detailsPrintlnGreen(getLang().getValue("general", "ok"));
				
    			Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());
    			Long diff = timestampAfter.getTime() - timestampBefore.getTime();
				ConsoleWriter.detailsPrint(getLang().getValue("general", "time").withParams(diff.toString()), 2);
				ConsoleWriter.detailsPrintLn("");
				
				index.addItem(obj);				
			}
		}

		if (countSave > 0) {
			index.saveDBIndex();
			index.addToGit();
		} else {
			ConsoleWriter.printlnRed(getLang().getValue("errors", "add", "cantFindObjectInDb").withParams(nameObj));
		}
		ConsoleWriter.println(getLang().getValue("general", "done"));
	}
}
