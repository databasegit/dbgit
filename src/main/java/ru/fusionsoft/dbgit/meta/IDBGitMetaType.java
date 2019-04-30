package ru.fusionsoft.dbgit.meta;

public interface IDBGitMetaType {
	public Class<?> getMetaClass();
	
	public Integer getPriority();
	
	public String getValue();
}
