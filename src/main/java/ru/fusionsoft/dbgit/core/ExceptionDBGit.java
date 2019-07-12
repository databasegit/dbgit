package ru.fusionsoft.dbgit.core;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;

/**
 * Base class for all Exception in dbgit project
 * 
 * @author mikle
 *
 */
public class ExceptionDBGit extends Exception {

	private static final long serialVersionUID = -4613368557825624023L;

	public ExceptionDBGit(String msg) {
		ConsoleWriter.printlnRed(msg);
		System.exit(1);
		//super(msg);
	}
	
	public ExceptionDBGit(String message, Throwable cause) {
		ConsoleWriter.printlnRed(message);
		System.exit(1);
		//super(message, cause);
	}
	
	public ExceptionDBGit(Throwable cause) {
		super(cause);
	}

}
