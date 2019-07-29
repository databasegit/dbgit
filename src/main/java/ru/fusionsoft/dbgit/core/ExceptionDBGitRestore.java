package ru.fusionsoft.dbgit.core;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class ExceptionDBGitRestore extends ExceptionDBGit {

	private static final long serialVersionUID = -8714585942496838509L;

	public ExceptionDBGitRestore(String msg) {
		super(msg);
		ConsoleWriter.println("\n" + msg);
	}
	
	public ExceptionDBGitRestore(String message, Throwable cause) {		
		super(message, cause);
		ConsoleWriter.println("\n" + message + "\n" + cause.getLocalizedMessage());
	}
	
	public ExceptionDBGitRestore(Throwable cause) {
		super(cause);
		ConsoleWriter.println(cause.getLocalizedMessage());
	}
}
