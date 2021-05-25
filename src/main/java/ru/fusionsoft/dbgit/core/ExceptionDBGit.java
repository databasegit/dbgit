package ru.fusionsoft.dbgit.core;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Base class for all Exception in dbgit project
 * 
 * @author mikle
 *
 */
public class ExceptionDBGit extends Exception {

	private static final long serialVersionUID = -4613368557825624023L;

	public ExceptionDBGit(Object msg) {
		super(String.valueOf(msg));
	}
	public ExceptionDBGit(Object message, Throwable cause) {
		super(String.valueOf(message), cause);
	}
	public ExceptionDBGit(Throwable cause) {
		super(cause);
	}



}
