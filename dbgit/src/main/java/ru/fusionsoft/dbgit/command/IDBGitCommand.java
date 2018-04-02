package ru.fusionsoft.dbgit.command;


/**
 * Class implementation Command dbgit
 * 
 * @author mikle
 *
 */
public interface IDBGitCommand {
	public void execute(String[] args) throws Exception;
}
