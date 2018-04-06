package ru.fusionsoft.dbgit.command;

import java.util.Map;

import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class CmdStatus implements IDBGitCommand {
	public void execute(String[] args) throws Exception {
		GitMetaDataManager gmdm = new GitMetaDataManager();
		
		Map<String, IMetaObject> dbObjs = gmdm.loadDBMetaData();		
		Map<String, IMetaObject> fileObjs = gmdm.loadFileMetaData();
		
		System.out.println("Changes databse objects not staged for commit:");
		for (String name : fileObjs.keySet()) {
			if (dbObjs.containsKey(name)) {
				if (!fileObjs.get(name).getHash().equals(dbObjs.get(name).getHash())) {
					System.out.println("   "+name);
				} else {
					System.out.println("   "+name+"   not found!!! ");
				}
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
