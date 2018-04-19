package ru.fusionsoft.dbgit.command;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class CmdDump implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdDump() {
		opts.addOption("a", false, "Added object to git");
		opts.addOption("f", false, "dump all objects where exists in dbgit index");
	}
	
	public String getCommandName() {
		return "dump";
	}
	
	public String getParams() {
		return "";
	}
	
	public String getHelperInfo() {
		return "Command for dump database objects to dbgit";
	}
	
	public Options getOptions() {
		return opts;
	}
	@Override
	public void execute(CommandLine cmdLine) throws Exception {		
		Boolean isAddToGit = cmdLine.hasOption('a');
		Boolean isAllDump = cmdLine.hasOption('f');
		
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
				
		DBGitIndex index = DBGitIndex.getInctance();

		IMapMetaObject fileObjs = gmdm.loadFileMetaData();
		
		for (IMetaObject obj : fileObjs.values()) {
			String hash = obj.getHash();
		
			obj.loadFromDB();
			if (isAllDump || !obj.getHash().equals(hash)) {
				//сохранили файл если хеш разный
				obj.saveToFile();
				index.addItem(obj);				
			
				if (isAddToGit) {
					obj.addToGit();						
				}
			}			
		}
		
		index.saveDBIndex();
		if (isAddToGit) {
			index.addToGit();
		}			
	}
}
