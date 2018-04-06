package ru.fusionsoft.dbgit.command;

import java.util.Map;

import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class CmdDump implements IDBGitCommand {

	@Override
	public void execute(String[] args) throws Exception {		
		GitMetaDataManager gmdm = new GitMetaDataManager();
		
		//TODO флаг по которому исправления добавляются в гит
				
		Map<String, IMetaObject> fileObjs = gmdm.loadFileMetaData();
		
		for (IMetaObject obj : fileObjs.values()) {
			String hash = obj.getHash();
			obj.loadFromDB();
			if (!obj.getHash().equals(hash)) {
				//сохранили файл если хеш разный
				obj.saveToFile();
				System.out.println("Save file "+obj.getName());
			}
			//TODO если флаг добавить в гит - то обновляем файл в индексе
		}
	}
}
