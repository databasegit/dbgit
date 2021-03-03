package ru.fusionsoft.dbgit.core;

public class ExceptionDBGitTableData extends Exception{
    private int errorFlag;

    public ExceptionDBGitTableData(int errorFlag) {
        this.errorFlag = errorFlag;
    }

    public ExceptionDBGitTableData(String message, int errorFlag) {
        super(message);
        this.errorFlag = errorFlag;
    }

    public ExceptionDBGitTableData(String message, Throwable cause, int errorFlag) {
        super(message, cause);
        this.errorFlag = errorFlag;
    }

    public ExceptionDBGitTableData(Throwable cause, int errorFlag) {
        super(cause);
        this.errorFlag = errorFlag;
    }

    public ExceptionDBGitTableData(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, int errorFlag) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.errorFlag = errorFlag;
    }
}
