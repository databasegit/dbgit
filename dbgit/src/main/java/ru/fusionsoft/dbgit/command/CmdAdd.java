package ru.fusionsoft.dbgit.command;

import java.util.Map;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaObjectFactory;

public class CmdAdd implements IDBGitCommand {

	public void execute(String[] args)  throws Exception {
		if (args.length == 0) {
			throw new ExceptionDBGit("Bad command. Not founnd object to add!");
		}
						
		String nameObj = args[0];
		
		//TODO ignore dbgit
		
		IMetaObject obj = MetaObjectFactory.createMetaObject(nameObj);
		obj.loadFromDB();
		
		obj.saveToFile();
		
		DBGit dbGit = DBGit.getInctance();
		dbGit.addFileToIndexGit(DBGitPath.DB_GIT_PATH+"/"+obj.getFileName());
	}
}
