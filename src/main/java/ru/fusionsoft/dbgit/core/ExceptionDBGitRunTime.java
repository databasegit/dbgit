package ru.fusionsoft.dbgit.core;


public class ExceptionDBGitRunTime extends RuntimeException {

	private static final long serialVersionUID = 958722213419205629L;

	public ExceptionDBGitRunTime(Object msg) {
		super(msg.toString());
	}

	public ExceptionDBGitRunTime(Object message, Throwable cause) {
		super(message.toString(), cause);
	}

	public ExceptionDBGitRunTime(Throwable cause) {
		super(cause);
	}
}
