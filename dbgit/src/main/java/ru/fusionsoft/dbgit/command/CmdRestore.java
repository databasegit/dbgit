package ru.fusionsoft.dbgit.command;

import java.io.IOException;
import java.util.Map;

import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.core.ItemIndex;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaObjectFactory;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

public class CmdRestore implements IDBGitCommand {

	public void execute(String[] args) throws ExceptionDBGit {
		GitMetaDataManager gmdm = new GitMetaDataManager();
		IMapMetaObject fileObjs = gmdm.loadFileMetaData();		
		IMapMetaObject updateObjs = new TreeMapMetaObject();
		IMapMetaObject deleteObjs = new TreeMapMetaObject();
		
		//delete obj
		DBGitIndex index = DBGitIndex.getInctance();
		for (ItemIndex item : index.getTreeItems().values()) {
			//TODO db ignore
			if (item.getIsDelete()) {
				try {
					IMetaObject obj = MetaObjectFactory.createMetaObject(item.getName());
					obj.loadFromDB();
					if (item.getHash().equals(obj.getHash())) {
						
					}
				} catch(ExceptionDBGit e) {
					LoggerUtil.getGlobalLogger().error("Error load and delete object: "+item.getName(), e);
				}
			}
		}		
		
		
		for (IMetaObject obj : fileObjs.values()) {
			String hash = obj.getHash();
			obj.loadFromDB();
			if (!obj.getHash().equals(hash)) {
				//запомнили файл если хеш разный
				
				updateObjs.put(obj.getName(), obj);
			}
		}
		
		gmdm.restoreDataBase(updateObjs);
	}


}
