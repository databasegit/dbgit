package ru.fusionsoft.dbgit.command;

import java.util.Map;

import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class CmdValid implements IDBGitCommand {

	public void execute(String[] args)  throws Exception {
		// TODO Auto-generated method stub
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
		
		//возможно за списком файлов нужно будет сходить в гит индекс
		Map<String, IMetaObject> fileObjs = gmdm.loadFileMetaData();
		
		
	}

}
