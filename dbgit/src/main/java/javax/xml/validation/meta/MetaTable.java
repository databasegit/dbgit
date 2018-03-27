package javax.xml.validation.meta;

import java.io.InputStream;
import java.io.OutputStream;

import ru.fusionsoft.dbgit.dbobjects.DBTable;

public class MetaTable implements IMetaObject {	
	
	private String name;
	
	//list fields
	//list indexes
	//list FKs
	
	public MetaTable(String namePath) {
		name = namePath;
	}
	
	public MetaTable(DBTable tbl) {
		name = tbl.getSchema()+"/"+tbl.getName()+"."+getType().getValue();
	}
	
	public DBGitMetaType getType() {
		return DBGitMetaType.DBGitTable;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
		
	}
	
	public String getFileName() {
		return "";
	}
	
	public void serialize(OutputStream stream) {
		//save to yaml
	}
	
	public void deSerialize(InputStream stream) {
		
	}
	
	public void loadFromDB() {
		
	}
	
	public void restoreDB() {
		
	}
	
	public String getHash() {
		return "";
	}
}
