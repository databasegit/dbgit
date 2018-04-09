package ru.fusionsoft.dbgit.command;

import java.util.Map;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaObjectFactory;
import ru.fusionsoft.dbgit.utils.MaskFilter;

public class CmdAdd implements IDBGitCommand {

	public void execute(String[] args)  throws Exception {
		if (args.length == 0) {
			throw new ExceptionDBGit("Bad command. Not founnd object to add!");
		}
						
		String nameObj = args[0];
		MaskFilter maskAdd = new MaskFilter(nameObj);
		
		DBGitIndex index = DBGitIndex.getInctance();
		DBGit dbGit = DBGit.getInctance();
		
		GitMetaDataManager gmdm = new GitMetaDataManager();		
		IMapMetaObject dbObjs = gmdm.loadDBMetaData();	
		
		Integer countSave = 0;
		
		for (IMetaObject obj : dbObjs.values()) {
			if (maskAdd.match(obj.getName())) {			
				obj.saveToFile();
								
				dbGit.addFileToIndexGit(DBGitPath.DB_GIT_PATH+"/"+obj.getFileName());				
				
				index.addItem(obj);
				countSave++;
			}
		}

		if (countSave > 0) {
			index.saveDBIndex();
			index.addToGit();
		}
	}
}
