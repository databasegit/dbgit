package ru.fusionsoft.dbgit.core;

public class ExceptionDBGitRunTime extends RuntimeException {

	private static final long serialVersionUID = 958722213419205629L;
	
	public ExceptionDBGitRunTime(String msg) {
		super(msg);
	}
	
	public ExceptionDBGitRunTime(String message, Throwable cause) {
		super(message, cause);
	}
	
	public ExceptionDBGitRunTime(Throwable cause) {
		super(cause);
	}

}
