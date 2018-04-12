package ru.fusionsoft.dbgit.command;

import java.util.Map;

import com.diogonunes.jcdp.color.api.Ansi.FColor;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdStatus implements IDBGitCommand {
	public void execute(String[] args) throws Exception {
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
		
		IMapMetaObject dbObjs = gmdm.loadDBMetaData();		
		IMapMetaObject fileObjs = gmdm.loadFileMetaData();
		DBGit dbGit = DBGit.getInctance();
		
		ConsoleWriter.println("Changes to be committed::");
		for (String name : dbGit.getAddedObjects(DBGitPath.DB_GIT_PATH)) {
			ConsoleWriter.printlnColor(name, FColor.GREEN, 1);		
		}
		ConsoleWriter.println(" ");
		
		ConsoleWriter.println("Changes databse objects not staged for commit:");
		for (String name : fileObjs.keySet()) {
			if (dbObjs.containsKey(name)) {
				if (!fileObjs.get(name).getHash().equals(dbObjs.get(name).getHash())) {
					ConsoleWriter.printlnColor(name, FColor.RED, 1);
				} 
			} else {
				ConsoleWriter.printlnColor(name, FColor.RED, 2);
			}
		}
		ConsoleWriter.println(" ");
				
		ConsoleWriter.println("Untracked databse objects:");
		for (String name : dbObjs.keySet()) {
			if (!fileObjs.containsKey(name)) {
				ConsoleWriter.println(name, 1);
			}
		}
		ConsoleWriter.println(" ");
	}
}
