package ru.fusionsoft.dbgit.utils;

import org.apache.commons.codec.binary.Base64;

public class Convertor {
	public static String EncodeBase64(String str) {
		if (str == null) return null;
		return Base64.encodeBase64String(str.getBytes());
	}
	
	public static String DecodeBase64(String str) {
		if (str == null) return null;
		return new String(Base64.decodeBase64(str));		
	}
	
	public static String getGUID() {
		return java.util.UUID.randomUUID().toString();
	}
}
