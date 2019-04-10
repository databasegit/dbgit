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
		return "mask_files";
	}
	
	public String getHelperInfo() {
		return "Command for add database objects to dbgit. You need to specify db object name as parameter in format just like it shows in status command output";
	}
	
	public Options getOptions() {
		return opts;
	}
	
	public void execute(CommandLine cmdLine)  throws Exception {		
		if (cmdLine.getArgs().length == 0) {
			throw new ExceptionDBGit("Bad command. Not found object to add!");
		}
						
		String nameObj = cmdLine.getArgs()[0];
		MaskFilter maskAdd = new MaskFilter(nameObj);
		
		DBGitIndex index = DBGitIndex.getInctance();
		
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();		
		IMapMetaObject dbObjs = gmdm.loadDBMetaData();	
		
		Integer countSave = 0;
		
		for (IMetaObject obj : dbObjs.values()) {
			if (maskAdd.match(obj.getName())) {			
				obj.saveToFile();
								
				countSave += obj.addToGit();				
				
				index.addItem(obj);
				
				ConsoleWriter.println("Add object to git: "+obj.getName());
			}
		}

		if (countSave > 0) {
			index.saveDBIndex();
			index.addToGit();
		} else {
			ConsoleWriter.printlnRed("Can't find object \"" + nameObj + "\" in database");
		}
	}
}
