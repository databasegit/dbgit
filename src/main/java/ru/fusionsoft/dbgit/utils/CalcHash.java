package ru.fusionsoft.dbgit.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;

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
			LoggerUtil.getGlobalLogger().error(DBGitLang.getInstance().getValue("errors", "hash", "errorSha").toString(), e);			
		}		
	}
	
	public static String byteToHex(byte[] bt) {
		StringBuilder hexString = new StringBuilder();
		for (byte b : bt) {
			hexString.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
		}
    	return hexString.toString();
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
			int n = data.length();
		} catch (Exception e) {
			LoggerUtil.getGlobalLogger().warn(DBGitLang.getInstance().getValue("errors", "hash", "nullParam").toString(), e);
			return this;				
		}

		try {
			return addData(data.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			LoggerUtil.getGlobalLogger().error(DBGitLang.getInstance().getValue("errors", "hash", "unsupportedEncode").toString(), e);
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
