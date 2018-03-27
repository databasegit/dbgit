package ru.fusionsoft.dbgit.utils;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CalcHash {
	private MessageDigest md;
	
	public CalcHash() {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
						

		} catch (NoSuchAlgorithmException e) {
			LoggerUtil.getGlobalLogger().error("error search MessageDigest SHA-256", e);			
		}		
	}
	
	public static String byteToHex(byte[] bt) {
		String hexString = "";
    	for (int i=0; i < bt.length; i++) {    	  
    	  hexString += Integer.toString( ( bt[i] & 0xff ) + 0x100, 16).substring( 1 );
    	}
    	return hexString;
	}
	
	public CalcHash addData(byte[] data) {
		md.update(data);	
		return this;
	}
	
	public CalcHash addData(InputStream data) {
		//md.update(data);
		//TODO
		return this;
	}
	
	public CalcHash addDataFile(String filename) {
		//md.update(data);
		//TODO
		return this;
	}
	
	
	public CalcHash addData(String data) {
		try {
			return addData(data.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			LoggerUtil.getGlobalLogger().error("UnsupportedEncodingException UTF-8", e);
			throw new RuntimeException(e);
		}
		
	}
	
	public byte[] calcHash() {
		return md.digest();		
	}
	
	public String calcHashStr() {
		return byteToHex(calcHash());		
	}
}
