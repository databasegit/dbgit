package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.MaskFilter;

public class CmdRm implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdRm() {
		opts.addOption("db", false, "Removes objects from index and from database");
	}
	
	public String getCommandName() {
		return "rm";
	}
	
	public String getParams() {
		return "<object>";
	}
	
	public String getHelperInfo() {
		//return "Command removes object from dbgit. Object you want to remove must be specified as parameter like here:\n"
		//		+ "    dbgit rm <object_name>\n"
		//		+ "    Object you want to remove must exist in index";
		return "Example:\n"
				+ "    dbgit rm <object_name>";
	}
	
	public Options getOptions() {
		return opts;
	}
	
	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		if (cmdLine.getArgs().length == 0) {
			throw new ExceptionDBGit("Bad command. Not found object to remove!");
		}
						
		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));
		
		String nameObj = cmdLine.getArgs()[0];
		MaskFilter maskAdd = new MaskFilter(nameObj);
		
		DBGitIndex index = DBGitIndex.getInctance();

		ConsoleWriter.detailsPrintLn("Checking files");

		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();		
		IMapMetaObject dbObjs = gmdm.loadFileMetaDataForce();
		IMapMetaObject deleteObjs = new TreeMapMetaObject();
		
		Integer countDelete = 0;
		
		ConsoleWriter.detailsPrintLn("Deleting");
		for (IMetaObject obj : dbObjs.values()) {
			if (maskAdd.match(obj.getName())) {										
				ConsoleWriter.detailsPrintLn("Processing object " + obj.getName());
				
				deleteObjs.put(obj);
				ConsoleWriter.detailsPrint("Removing from git...", 2);				
				countDelete += obj.removeFromGit();					
				ConsoleWriter.detailsPrintlnGreen("OK");
				
				ConsoleWriter.detailsPrint("Removing from index...", 2);
				index.deleteItem(obj);
				ConsoleWriter.detailsPrintlnGreen("OK");
			}
		}
		if (cmdLine.hasOption("db")) {
			ConsoleWriter.detailsPrint("Removing from db...", 2);
			gmdm.deleteDataBase(deleteObjs);
			ConsoleWriter.detailsPrintlnGreen("OK");
		}
		
		if (countDelete > 0) {
			index.saveDBIndex();
			index.addToGit();
		} else {
			ConsoleWriter.printlnRed("Can't find file \"" + nameObj + "\" in index");
		}
		ConsoleWriter.println("Done!");
	}


}
