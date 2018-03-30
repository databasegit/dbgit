package ru.fusionsoft.dbgit.command;

import java.util.Map;

import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class CmdStatus implements IDBGitCommand {
	public void execute(String[] args) {
		GitMetaDataManager gmdm = new GitMetaDataManager();
		
		Map<String, IMetaObject> dbObjs = gmdm.loadDBMetaData();
		
		//возможно за списком файлов нужно будет сходить в гит индекс
		Map<String, IMetaObject> fileObjs = gmdm.loadFileMetaData();
		
		//show diff from dbObjs and fileObjs
		
	}
}
