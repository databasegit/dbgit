package ru.fusionsoft.dbgit.core;

import org.slf4j.Logger;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

public class ExceptionDBGitRunTime extends RuntimeException {

	private static final long serialVersionUID = 958722213419205629L;
	private Logger logger = LoggerUtil.getLogger(this.getClass());
	
	public ExceptionDBGitRunTime(String msg) {
		super(msg);
	}
	
	public ExceptionDBGitRunTime(String message, Throwable cause) {
		ConsoleWriter.printlnRed(message);
		logger.error(message, cause);
		System.exit(1);
		//super(message, cause);
	}
	
	public ExceptionDBGitRunTime(Throwable cause) {
		//super(cause);
		logger.error(cause.getLocalizedMessage(), cause);
	}

}
