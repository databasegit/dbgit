package ru.fusionsoft.dbgit.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Class utility for calculate hash 
 * 
 * @author mikle
 *
 */
public class CalcHash {
	private MessageDigest md;
	
	public CalcHash() {
		try {
			md = MessageDigest.getInstance("SHA-256");						
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
	
	public CalcHash addData(InputStream stream) throws Exception {
		byte[] buffer = new byte[8 * 1024];
	    int bytesRead;
	    while ((bytesRead = stream.read(buffer)) != -1) {
	    	md.update(buffer, 0, bytesRead);
	    }

		return this;
	}
	
	public CalcHash addDataFile(String filename) throws Exception {
		InputStream stream = new FileInputStream(new File(filename));
		addData(stream);	    
		stream.close();
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
