package ru.fusionsoft.dbgit.command;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitObjectNotFound;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.core.ItemIndex;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaObjectFactory;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

public class CmdRestore implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdRestore() {
		
	}
	
	public String getCommandName() {
		return "restore";
	}
	
	public String getParams() {
		return "";
	}
	
	public String getHelperInfo() {
		return "Command restore database";
	}
	
	public Options getOptions() {
		return opts;
	}
	
	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
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
						deleteObjs.put(obj);
					}
				} catch(ExceptionDBGit e) {
					LoggerUtil.getGlobalLogger().error("Error load and delete object: "+item.getName(), e);
				}
			}
		}
		gmdm.deleteDataBase(deleteObjs);
		
		
		for (IMetaObject obj : fileObjs.values()) {
			Boolean isRestore = false;
			String hash = obj.getHash();
			try {
				IMetaObject dbObj = MetaObjectFactory.createMetaObject(obj.getName());
				dbObj.loadFromDB();
				isRestore = !obj.getHash().equals(hash);
			} catch (ExceptionDBGit e) {
				isRestore = true;
			} catch (ExceptionDBGitRunTime e) {
				isRestore = true;
			}
			if (isRestore) {
				//запомнили файл если хеш разный или объекта нет				
				updateObjs.put(obj);
			}
		}
		
		gmdm.restoreDataBase(updateObjs);
	}


}
