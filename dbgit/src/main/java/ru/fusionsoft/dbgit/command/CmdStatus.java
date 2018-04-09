package ru.fusionsoft.dbgit.command;

import java.util.Map;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class CmdStatus implements IDBGitCommand {
	public void execute(String[] args) throws Exception {
		GitMetaDataManager gmdm = new GitMetaDataManager();
		
		IMapMetaObject dbObjs = gmdm.loadDBMetaData();		
		IMapMetaObject fileObjs = gmdm.loadFileMetaData();
		DBGit dbGit = DBGit.getInctance();
		
		System.out.println("Changes to be committed::");
		for (String name : dbGit.getAddedObjects(DBGitPath.DB_GIT_PATH)) {
			System.out.println("   "+name);
		}
		
		System.out.println("Changes databse objects not staged for commit:");
		for (String name : fileObjs.keySet()) {
			if (dbObjs.containsKey(name)) {
				if (!fileObjs.get(name).getHash().equals(dbObjs.get(name).getHash())) {
					System.out.println("   "+name);
				} 
			} else {
				System.out.println("   "+name+"   not found!!! ");
			}
		}
				
		System.out.println("Untracked databse objects:");
		for (String name : dbObjs.keySet()) {
			if (!fileObjs.containsKey(name)) {
				System.out.println("   "+name);
			}
		}
	}
}
