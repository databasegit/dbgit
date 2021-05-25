package ru.fusionsoft.dbgit.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.diogonunes.jcdp.color.ColoredPrinter;
import com.diogonunes.jcdp.color.api.Ansi.Attribute;
import com.diogonunes.jcdp.color.api.Ansi.BColor;
import com.diogonunes.jcdp.color.api.Ansi.FColor;

import java.text.MessageFormat;

public class ConsoleWriter {
	private static Logger logger = LoggerUtil.getLogger(ConsoleWriter.class);
	private static ColoredPrinter cp = new ColoredPrinter.Builder(1, false).build();
	private static boolean showDetailedLog = false;


	public static void print(Object message, FColor color, int level, boolean newLine, boolean onlyDetailed){
		if(onlyDetailed && !showDetailedLog) return;

		String tab = StringUtils.leftPad("", 4*level, " ");
		String msg = MessageFormat.format("{0}{1}",
			tab,
			message.toString()
		);
		System.out.print(( newLine ? "\n" : " " ) + msg );
//		cp.print(msg, Attribute.NONE, color, BColor.BLACK);
//		cp.clear();

	}

	public static void setDetailedLog(boolean toShowLog) {
		showDetailedLog = toShowLog;
	}
	public static boolean getDetailedLog() {
		return showDetailedLog;
	}

	// Just print
	// With no color, with color and with hardcoded colors
	// - no levels cause them mean nothing without a newline

	public static void print(Object msg){
		print(msg, FColor.WHITE, 0, false, false);
	}

	public static void printColor(Object msg, FColor color) {
		print(msg, color, 0, false, false);
	}

	public static void printGreen(Object msg) {
		print(msg, FColor.GREEN, 0, false, false);
	}

	public static void printRed(Object msg) {
		print(msg, FColor.RED, 0, false, false);
	}

	// Print with a newline
	// With no color, with color and with hardcoded colors
	// - with explicit level

	public static void printLineBreak(){println("", 0);}

	public static void println(Object msg, Integer level) {
		print(msg, FColor.WHITE, level, true, false);
	}

	public static void printlnColor(Object msg, FColor color, Integer level) {
		print(msg, color, level, true, false);
	}

	public static void printlnGreen(Object msg, int level) {
		print(msg, FColor.GREEN, level, true, false);
	}

	public static void printlnRed(Object msg, int level) {
		print(msg, FColor.RED, level, true, false);
	}


	// Detailed versions of other methods
	// - print if only showDetailedLog is true

	public static void detailsPrint(Object msg){
		print(msg, FColor.WHITE, 0, false, true);
	}

	public static void detailsPrintColor(Object msg, FColor color) {
		print(msg, color, 0, false, true);
	}

	public static void detailsPrintGreen(Object msg) {
		print(msg, FColor.GREEN, 0, false, true);
	}

	public static void detailsPrintRed(Object msg) {
		print(msg, FColor.RED, 0, false, true);
	}

	public static void detailsPrintln(Object msg, Integer level) {
		print(msg, FColor.WHITE, level, true, true);
	}

	public static void detailsPrintlnColor(Object msg, FColor color, Integer level) {
		print(msg, color, level, true, true);
	}

	public static void detailsPrintlnGreen(Object msg, int level) {
		print(msg, FColor.GREEN, level, true, true);
	}

	public static void detailsPrintlnRed(Object msg, int level) {
		print(msg, FColor.RED, level, true, true);
	}


}
