package ru.fusionsoft.dbgit.command;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
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
		opts.addOption("s", true, "Save command restore to file");
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
		AdapterFactory.createAdapter();
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
		IMapMetaObject fileObjs = gmdm.loadFileMetaData();		
		IMapMetaObject updateObjs = new TreeMapMetaObject();
		IMapMetaObject deleteObjs = new TreeMapMetaObject();
		
		FileOutputStream fop = null;
		if (cmdLine.hasOption("s")) {
			IDBAdapter adapter = AdapterFactory.createAdapter();
			String scriptName = cmdLine.getOptionValue("s");
			
			File file = new File(scriptName);
			if (!file.exists()) {
				file.createNewFile();
			}
			
			fop = new FileOutputStream(file);

			adapter.setDumpSqlCommand(fop, false);
		}
		try {
			//delete obj
			DBGitIndex index = DBGitIndex.getInctance();
			for (ItemIndex item : index.getTreeItems().values()) {
				//TODO db ignore
				if (item.getIsDelete()) {
					try {
						IMetaObject obj = MetaObjectFactory.createMetaObject(item.getName());
						gmdm.loadFromDB(obj);					
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
				try {
					IMetaObject dbObj = MetaObjectFactory.createMetaObject(obj.getName());				
					gmdm.loadFromDB(dbObj);
					isRestore = !dbObj.getHash().equals(obj.getHash());
				} catch (ExceptionDBGit e) {
					isRestore = true;
					e.printStackTrace();
				} catch (ExceptionDBGitRunTime e) {
					isRestore = true;
					e.printStackTrace();
				}
				if (isRestore) {
					//запомнили файл если хеш разный или объекта нет				
					updateObjs.put(obj);
				}
			}
			
			gmdm.restoreDataBase(updateObjs);
		} finally {
			if (fop != null) {
				fop.flush();
				fop.close();
			}	
		}		
	}


}
