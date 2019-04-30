package ru.fusionsoft.dbgit.core;

public class ExceptionDBGitObjectNotFound extends ExceptionDBGit  {

	private static final long serialVersionUID = 2163408974338332577L;

	public ExceptionDBGitObjectNotFound(String msg) {
		super(msg);
	}
	
	public ExceptionDBGitObjectNotFound(String message, Throwable cause) {
		super(message, cause);
	}
	
	public ExceptionDBGitObjectNotFound(Throwable cause) {
		super(cause);
	}
}
