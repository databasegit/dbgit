package ru.fusionsoft.dbgit.core;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

import java.sql.SQLException;
public class ExceptionDBGitRunTime extends RuntimeException {

	private static final long serialVersionUID = 958722213419205629L;
	private ExceptionDBGit exceptionDBGit;

	public ExceptionDBGitRunTime(Object msg) {
		super(msg.toString());
		exceptionDBGit = new ExceptionDBGit(this);
	}

	public ExceptionDBGitRunTime(Object message, Throwable cause) {
		super(message.toString(), cause);
		exceptionDBGit = new ExceptionDBGit(this);
	}

	public ExceptionDBGitRunTime(Throwable cause) {
		super(cause);
		exceptionDBGit = new ExceptionDBGit(this);
	}
}
