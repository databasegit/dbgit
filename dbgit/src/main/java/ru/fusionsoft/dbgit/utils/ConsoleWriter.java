package ru.fusionsoft.dbgit.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.diogonunes.jcdp.color.ColoredPrinter;
import com.diogonunes.jcdp.color.api.Ansi.Attribute;
import com.diogonunes.jcdp.color.api.Ansi.BColor;
import com.diogonunes.jcdp.color.api.Ansi.FColor;

public class ConsoleWriter {
	private static Logger logger = LoggerUtil.getLogger(ConsoleWriter.class);
	private static ColoredPrinter cp = new ColoredPrinter.Builder(1, false).build();
	
	public static void printlnGreen(String msg) {
		printlnColor(msg, FColor.GREEN, 0);
	}
	
	public static void printlnRed(String msg) {
		printlnColor(msg, FColor.RED, 0);
	}
	
	public static void printlnColor(String msg, FColor color, Integer level) {
		String tab = StringUtils.leftPad("", 4*level, " ");
		cp.println(tab+msg, Attribute.NONE, color, BColor.BLACK);
		cp.clear();
		logger.info(msg);
	}
	
	public static void printColor(String msg, FColor color, Integer level) {
		String tab = StringUtils.leftPad("", 4*level, " ");
		cp.print(tab+msg, Attribute.NONE, color, BColor.BLACK);
		cp.clear();
		logger.info(msg);
	}
	
	public static void println(String msg) {
		println(msg, 0);
	}
	public static void println(String msg, Integer level) {
		String tab = StringUtils.leftPad("", 4*level, " ");
		cp.print(tab+msg);
		logger.info(msg);
	}
	
	public static void print(String msg, Integer level) {
		String tab = StringUtils.leftPad("", 4*level, " ");
		cp.print(tab+msg);
		logger.info(msg);
	}
}
