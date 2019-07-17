package ru.fusionsoft.dbgit.core;

import org.slf4j.Logger;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

/**
 * Base class for all Exception in dbgit project
 * 
 * @author mikle
 *
 */
public class ExceptionDBGit extends Exception {

	private static final long serialVersionUID = -4613368557825624023L;
	private Logger logger = LoggerUtil.getLogger(this.getClass());

	public ExceptionDBGit(Object msg) {
		ConsoleWriter.printlnRed(msg);
		System.exit(1);
		//super(msg);
	}
	
	public ExceptionDBGit(String msg) {
		ConsoleWriter.printlnRed(msg);
		System.exit(1);
		//super(msg);
	}
	
	public ExceptionDBGit(String message, Throwable cause) {
		ConsoleWriter.printlnRed(message);
		logger.error(message, cause);
		System.exit(1);
		//super(message, cause);
	}
	
	public ExceptionDBGit(Throwable cause) {
		logger.error(cause.getLocalizedMessage(), cause);
		//super(cause);
	}

}
