package ru.fusionsoft.dbgit.command;

import java.util.Map;

import javax.xml.validation.meta.IMetaObject;

import ru.fusionsoft.dbgit.core.GitMetaDataManager;

public class CmdValid implements IDBGitCommand {

	public void execute(String[] args)  throws Exception {
		// TODO Auto-generated method stub
		GitMetaDataManager gmdm = new GitMetaDataManager();
		
		//возможно за списком файлов нужно будет сходить в гит индекс
		Map<String, IMetaObject> fileObjs = gmdm.loadFileMetaData();
		
		
	}

}
