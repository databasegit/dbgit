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
	private static boolean showDetailedLog = false;
	
	
	public static void detailsPrintLn(Object msg) {
		if (showDetailedLog)
			println(msg.toString());
	}

	public static void detailsPrint(Object msg, int level) {
		if (showDetailedLog)
			print(msg.toString(), level);
	}

	public static void detailsPrintlnGreen(Object msg) {
		if (showDetailedLog)
			printlnColor(msg.toString(), FColor.GREEN, 0);
	}
	
	public static void detailsPrintlnRed(Object msg) {
		if (showDetailedLog)
			printlnColor(msg.toString(), FColor.RED, 0);
	}

	public static void printlnGreen(Object msg) {
		printlnColor(msg.toString(), FColor.GREEN, 0);
	}
	
	public static void printlnRed(Object msg) {
		printlnColor(msg.toString(), FColor.RED, 0);
	}
	
	public static void detailsPrintLn(String msg) {
		if (showDetailedLog)
			println(msg);
	}

	public static void detailsPrint(String msg, int level) {
		if (showDetailedLog)
			print(msg, level);
	}

	public static void detailsPrintlnGreen(String msg) {
		if (showDetailedLog)
			printlnColor(msg, FColor.GREEN, 0);
	}
	
	public static void detailsPrintlnRed(String msg) {
		if (showDetailedLog)
			printlnColor(msg, FColor.RED, 0);
	}

	public static void printlnGreen(String msg) {
		printlnColor(msg, FColor.GREEN, 0);
	}
	
	public static void printlnRed(String msg) {
		printlnColor(msg, FColor.RED, 0);
	}
	
	public static void printlnColor(String msg, FColor color, Integer level) {
		String tab = StringUtils.leftPad("", 4*level, " ");
		/*
		System.out.println(tab + msg);
		if (1==1) return ;
		*/
		cp.println(tab+msg, Attribute.NONE, color, BColor.BLACK);
		cp.clear();
		logger.info(msg);
	}
	
	public static void printColor(String msg, FColor color, Integer level) {
		String tab = StringUtils.leftPad("", 4*level, " ");
		/*
		System.out.println(tab + msg);
		if (1==1) return ;
		*/
		cp.print(tab+msg, Attribute.NONE, color, BColor.BLACK);
		cp.clear();
		logger.info(msg);		
	}
	
	public static void println(Object msg) {
		println(msg.toString(), 0);
	}

	public static void println(String msg) {
		println(msg, 0);
	}

	public static void println(String msg, Integer level) {
		String tab = StringUtils.leftPad("", 4*level, " ");
		/*
		System.out.println(tab + msg);
		if (1==1) return ;
		*/
		cp.println(tab+msg);
		cp.clear();
		logger.info(msg);
	}
	
	public static void print(String msg, Integer level) {
		String tab = StringUtils.leftPad("", 4*level, " ");
		/*
		System.out.println(tab + msg);
		if (1==1) return ;
		*/
		cp.print(tab+msg);
		cp.clear();
		logger.info(msg);
	}
	
	public static void setDetailedLog(boolean toShowLog) {
		showDetailedLog = toShowLog;
	}

	public static boolean getDetailedLog() {
		return showDetailedLog;
	}
}
