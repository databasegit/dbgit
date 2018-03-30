package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface IMetaObject {	
	public DBGitMetaType getType();
	
	public String getName();
	public void setName(String name);
	
	public String getFileName();
		
	public void serialize(OutputStream stream) throws IOException;
	
	public IMetaObject deSerialize(InputStream stream) throws IOException;
	
	public void loadFromDB();
		
	public String getHash();
	
	default void saveToFile(String basePath) {
		System.out.println("I1 logging::");
	}
	
	default void loadFromFile(String basePath) {
		System.out.println("I1 logging::");
	}
}
