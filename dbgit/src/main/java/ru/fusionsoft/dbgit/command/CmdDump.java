package ru.fusionsoft.dbgit.command;

import java.util.Map;

import javax.xml.validation.meta.IMetaObject;

import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;

public class CmdDump implements IDBGitCommand {

	public void execute(String[] args) {
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
