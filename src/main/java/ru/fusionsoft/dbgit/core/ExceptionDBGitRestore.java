package ru.fusionsoft.dbgit.core;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class ExceptionDBGitRestore extends ExceptionDBGit {

	private static final long serialVersionUID = -8714585942496838509L;

	public ExceptionDBGitRestore(Object msg) { super(msg); }
	public ExceptionDBGitRestore(Object message, Throwable cause) { super(message, cause); }
	public ExceptionDBGitRestore(Throwable cause) { super(cause); }

	@Override public void printMessageAndStackTrace(){
		printFail();
		super.printMessageAndStackTrace();
	}

	private void printFail(){
		ConsoleWriter.detailsPrintRed(DBGitLang.getInstance()
			.getValue("errors", "meta", "fail")
		);
	}
}
