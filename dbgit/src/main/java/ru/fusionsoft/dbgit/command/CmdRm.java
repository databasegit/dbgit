package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.MaskFilter;

public class CmdRm implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdRm() {
		
	}
	
	public String getCommandName() {
		return "rm";
	}
	
	public String getParams() {
		return "object";
	}
	
	public String getHelperInfo() {
		return "Command removes object from dbgit. Object you want to remove must be specified as parameter like here:\n"
				+ "    dbgit rm <object_name>\n"
				+ "Object you want to remove must exist in index";
	}
	
	public Options getOptions() {
		return opts;
	}
	
	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		if (cmdLine.getArgs().length == 0) {
			throw new ExceptionDBGit("Bad command. Not found object remove!");
		}
						
		String nameObj = cmdLine.getArgs()[0];
		MaskFilter maskAdd = new MaskFilter(nameObj);
		
		DBGitIndex index = DBGitIndex.getInctance();
		
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();		
		IMapMetaObject dbObjs = gmdm.loadFileMetaData();
		IMapMetaObject deleteObjs = new TreeMapMetaObject();
		
		Integer countDelete = 0;
		
		for (IMetaObject obj : dbObjs.values()) {
			if (maskAdd.match(obj.getName())) {										
				
				deleteObjs.put(obj);
				countDelete += obj.removeFromGit();						
				index.deleteItem(obj);
			}
		}
		
		gmdm.deleteDataBase(deleteObjs);

		if (countDelete > 0) {
			index.saveDBIndex();
			index.addToGit();
		} else {
			ConsoleWriter.printlnRed("Can't find file \"" + nameObj + "\" in index");
		}
		
	}


}
