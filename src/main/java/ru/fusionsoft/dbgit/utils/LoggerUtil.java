package ru.fusionsoft.dbgit.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerUtil {
	
	private static Logger globalLogger = LoggerFactory.getLogger("GLOBAL");
	
	@SuppressWarnings("rawtypes")
	public static Logger getLogger(Class cl) {
		return LoggerFactory.getLogger(cl);
	}
	
	public static Logger getGlobalLogger() {
		return globalLogger;
	}
}
