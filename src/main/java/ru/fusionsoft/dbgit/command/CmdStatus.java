package ru.fusionsoft.dbgit.command;

import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.diogonunes.jcdp.color.api.Ansi.FColor;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTableData;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdStatus implements IDBGitCommand {
	
	private Options opts = new Options();
	
	public CmdStatus() {
		
	}
	
	public String getCommandName() {
		return "status";
	}
	
	public String getParams() {
		return "";
	}
	
	public String getHelperInfo() {
		return getLang().getValue("help", "status").toString();
	}
	
	public Options getOptions() {
		return opts;
	}
	
	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
		
		IMapMetaObject dbObjs = gmdm.loadDBMetaData();		
		IMapMetaObject fileObjs = gmdm.loadFileMetaDataForce();
		IMapMetaObject changeObjs = new TreeMapMetaObject();
		IMapMetaObject addedObjs = new TreeMapMetaObject();
		DBGit dbGit = DBGit.getInstance();
		boolean hasConflicts = DBGitIndex.getInctance().hasConflicts();
		String repoVersion = DBGitIndex.getInctance().getRepoVersion();
		ConsoleWriter.println(getLang().getValue("general", "status", "repVersion").withParams(repoVersion));
		ConsoleWriter.println(getLang().getValue("general", "status", "dbgitVersion").withParams(DBGitIndex.VERSION));
		
		if (!DBGitIndex.getInctance().isCorrectVersion())
			ConsoleWriter.println(getLang().getValue("general", "status", "differentVersions"));
		
		if (hasConflicts) {
			ConsoleWriter.println(getLang().getValue("general", "status", "conflicts"));
		}
		
		SchemaSynonym ss = SchemaSynonym.getInctance();
				
		if (ss.getCountSynonym() > 0) {
			ConsoleWriter.printlnGreen(getLang().getValue("general", "status", "usedSynonyms"));
			ConsoleWriter.printlnGreen(getLang().getValue("general", "status", "synSchema"));
			for (Entry<String, String> el : ss.getMapSchema().entrySet()) {
				ConsoleWriter.println(el.getKey() + " - " + el.getValue());
			}			
		}
		
		for (String name : fileObjs.keySet()) {
			if (dbObjs.containsKey(name)) {
				if (!fileObjs.get(name).getHash().equals(dbObjs.get(name).getHash())) {
					changeObjs.put(dbObjs.get(name));
					
					/*
					//debug find diff
					if (fileObjs.get(name) instanceof MetaTableData && dbObjs.get(name) instanceof MetaTableData) {
						System.out.println(name);
						MetaTableData d1 = (MetaTableData)fileObjs.get(name);
						MetaTableData d2 = (MetaTableData)dbObjs.get(name);
						d1.diff(d2);
					}
					*/
					/*
					System.out.println(name);
					System.out.println("file: "+fileObjs.get(name).getHash());
					System.out.println("bd: "+dbObjs.get(name).getHash());
					*/
				} 
			} else {		
				changeObjs.put(fileObjs.get(name));
			}
		}
		
		
		for (String name : dbGit.getAddedObjects(DBGitPath.DB_GIT_PATH)) {
			if (fileObjs.containsKey(name) && !changeObjs.containsKey(name))
				addedObjs.put(fileObjs.get(name));					
		}
		
		ConsoleWriter.println(getLang().getValue("general", "status", "changesToCommit"));
		for(IMetaObject obj : addedObjs.values()) {
			printObect(obj, FColor.GREEN, 1);
		}
		ConsoleWriter.println(" ");

		ConsoleWriter.println(getLang().getValue("general", "status", "notStaged"));
		for(IMetaObject obj : changeObjs.values()) {
			printObect(obj, FColor.RED, 1);
		}
		ConsoleWriter.println(" ");
		
				
		ConsoleWriter.println(getLang().getValue("general", "status", "untracked"));
		for (String name : dbObjs.keySet()) {
			if (!fileObjs.containsKey(name)) {
				ConsoleWriter.println(name, 1);
			}
		}
		ConsoleWriter.println(" ");

		ConsoleWriter.println(getLang().getValue("general", "status", "localNotStaged"));
		for (String modified : DBGit.getInstance().getModifiedFiles()) {
			ConsoleWriter.printlnColor(modified, FColor.RED, 1);
		}
		
		ConsoleWriter.println(" ");

		ConsoleWriter.println(getLang().getValue("general", "status", "localToCommit"));
		for (String modified : DBGit.getInstance().getChanged()) {
			ConsoleWriter.printlnColor(modified, FColor.GREEN, 1);
		}

		ConsoleWriter.println(" ");
	}
	
	public void printObect(IMetaObject obj, FColor color, Integer level) {
		ConsoleWriter.printlnColor(obj.getName() /*+ " ("+obj.getHash()+")"*/, color, level);
	}
}
