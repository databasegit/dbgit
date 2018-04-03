package ru.fusionsoft.dbgit.command;

import java.util.Map;

import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class CmdDump implements IDBGitCommand {

	@Override
	public void execute(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		GitMetaDataManager gmdm = new GitMetaDataManager();
		
		//возможно за списком файлов нужно будет сходить в гит индекс
		
		Map<String, IMetaObject> fileObjs = gmdm.loadFileMetaData();
		
		
		for (IMetaObject obj : fileObjs.values()) {
			String hash = obj.getHash();
			obj.loadFromDB();
			if (!obj.getHash().equals(hash)) {
				//сохранили файл если хеш разный
				obj.saveToFile(DBGitPath.getFullPath(DBGitPath.OBJECTS_PATH));
			}
		}
		

	}

}
