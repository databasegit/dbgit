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
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.utils.MaskFilter;

public class CmdRm implements IDBGitCommand {

	public void execute(String[] args)  throws Exception {
		if (args.length == 0) {
			throw new ExceptionDBGit("Bad command. Not founnd object remove!");
		}
						
		String nameObj = args[0];
		MaskFilter maskAdd = new MaskFilter(nameObj);
		
		DBGitIndex index = DBGitIndex.getInctance();
		DBGit dbGit = DBGit.getInctance();
		
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();		
		IMapMetaObject dbObjs = gmdm.loadFileMetaData();
		IMapMetaObject deleteObjs = new TreeMapMetaObject();
		
		Integer countDelete = 0;
		
		for (IMetaObject obj : dbObjs.values()) {
			if (maskAdd.match(obj.getName())) {										
				
				deleteObjs.put(obj);
				dbGit.removeFileFromIndexGit(DBGitPath.DB_GIT_PATH+"/"+obj.getFileName());				
				
				index.deleteItem(obj);
				countDelete++;
			}
		}
		
		gmdm.deleteDataBase(deleteObjs);

		if (countDelete > 0) {
			index.saveDBIndex();
			index.addToGit();
		}
		
	}


}
