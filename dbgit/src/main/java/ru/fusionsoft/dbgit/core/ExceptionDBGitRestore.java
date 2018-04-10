package ru.fusionsoft.dbgit.core;

public class ExceptionDBGitRestore extends ExceptionDBGit {

	private static final long serialVersionUID = -8714585942496838509L;

	public ExceptionDBGitRestore(String msg) {
		super(msg);
	}
	
	public ExceptionDBGitRestore(String message, Throwable cause) {
		super(message, cause);
	}
	
	public ExceptionDBGitRestore(Throwable cause) {
		super(cause);
	}
}
