package ru.fusionsoft.dbgit.utils;

public class MaskFilter {
	private String mask;
	private String regex;
	
	public MaskFilter(String mask) {
		this.mask = mask;
		
		regex = mask.replace("/", "\\/").replace(".", "\\.").replace("?", ".?").replace("*", ".*"); 
		System.out.println(regex);
	}
	
	public boolean match(String exp) {
		String tmp = exp.replace("\\", "/");
		System.out.println(tmp);
		return tmp.matches(regex);
	}

	public String getMask() {
		return mask;
	}
}
