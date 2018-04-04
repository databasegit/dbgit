package ru.fusionsoft.dbgit.core;

/**
 * Base class for all Exception in dbgit project
 * 
 * @author mikle
 *
 */
public class ExceptionDBGit extends Exception {

	private static final long serialVersionUID = -4613368557825624023L;

	public ExceptionDBGit(String msg) {
		super(msg);
	}
	
	public ExceptionDBGit(String message, Throwable cause) {
		super(message, cause);
	}
	
	public ExceptionDBGit(Throwable cause) {
		super(cause);
	}

}
