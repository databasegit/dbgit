package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;

/**
 * Class implementation Command dbgit
 * 
 * @author mikle
 *
 */
public interface IDBGitCommand {
	public void execute(CommandLine cmdLine) throws Exception;
	
	public String getCommandName();
	
	public String getParams();
	
	public String getHelperInfo();
	
	public Options getOptions();
	
	public default DBGitLang getLang() {
		return DBGitLang.getInstance();
	}
	
	public default void checkVersion() throws ExceptionDBGit {
		if (!DBGitIndex.getInctance().isCorrectVersion())
			throw new ExceptionDBGit(getLang().getValue("errors", "incorrectVersion").withParams(DBGitIndex.VERSION, DBGitIndex.getInctance().getRepoVersion()));

	}
}
