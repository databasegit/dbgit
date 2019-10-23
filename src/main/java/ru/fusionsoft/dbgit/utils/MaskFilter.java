package ru.fusionsoft.dbgit.utils;

public class MaskFilter {
	private String mask;
	private String regex;
	
	public MaskFilter(String mask) {
		this.mask = mask;
		
		regex = "(?i)" + mask.replace("/", "\\/")
				.replace(".", "\\.")
				.replace("?", ".?")
				.replace("*", ".*")
				.replace("$", "\\$")
				.replaceFirst("\"", "(?-i)")
				.replaceFirst("\"", "(?i)")
		; 
	}
	
	public boolean match(String exp) {
		String tmp = exp.replace("\\", "/");
		return tmp.matches(regex);
	}

	public String getMask() {
		return mask;
	}
}
