package ru.fusionsoft.dbgit.utils;

public class Util {
	public static <T> T nvl(T arg0, T arg1) {
	    return (arg0 == null) ? arg1 : arg0;
	}
}
