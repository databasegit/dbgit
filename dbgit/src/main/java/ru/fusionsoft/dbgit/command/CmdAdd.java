package ru.fusionsoft.dbgit.command;

import java.util.Map;

import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaObjectFactory;

public class CmdAdd implements IDBGitCommand {

	public void execute(String[] args)  throws Exception {
		// TODO Auto-generated method stub
		GitMetaDataManager gmdm = new GitMetaDataManager();
		
		//возжно позже оптимизация
		//Map<String, IMetaObject> dbObjs = gmdm.loadDBMetaData();
		
		String name = "name from args";
		
		IMetaObject obj = MetaObjectFactory.createMetaObject(name);
		obj.loadFromDB();
		
		//obj.serialize(stream);
		
		//find obj by name and save to file 
		//добавить в гит этот файл
	}


}
