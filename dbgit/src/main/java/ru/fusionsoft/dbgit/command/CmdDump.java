package ru.fusionsoft.dbgit.command;

import java.util.Map;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class CmdDump implements IDBGitCommand {

	@Override
	public void execute(String[] args) throws Exception {		
		Boolean isAddToGit = (args.length > 1 && args[0].equals("-a")); 
		
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
				
		DBGitIndex index = DBGitIndex.getInctance();
		DBGit dbGit = DBGit.getInctance();

		IMapMetaObject fileObjs = gmdm.loadFileMetaData();
		
		for (IMetaObject obj : fileObjs.values()) {
			String hash = obj.getHash();
		
			obj.loadFromDB();
			if (!obj.getHash().equals(hash)) {
				//сохранили файл если хеш разный
				obj.saveToFile();
				index.addItem(obj);				
			
				if (isAddToGit) {
					dbGit.addFileToIndexGit(DBGitPath.DB_GIT_PATH+"/"+obj.getFileName());
				}
			}			
		}
		
		index.saveDBIndex();
		if (isAddToGit) {
			index.addToGit();
		}			
	}
}
