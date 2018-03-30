package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;

public class MetaTable extends MetaBase {	

	private DBTable table;
	
	private Map<String, DBTableField> fields = new HashMap<String, DBTableField>();
	private Map<String, DBIndex> indexes = new HashMap<String, DBIndex>();
	private Map<String, DBConstraint> constraints = new HashMap<String, DBConstraint>();
	
	public MetaTable() {	
	}
	
	public MetaTable(String namePath) {
		this.name = namePath;
	}
	
	public MetaTable(DBTable tbl) {
		setTable(tbl);
	}
	
	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DBGitTable;
	}
	
	@Override
	public void serialize(OutputStream stream) throws IOException {
		yamlSerialize(stream);
	}

	@Override
	public IMetaObject deSerialize(InputStream stream) throws IOException {
		return yamlDeSerialize(stream);
	}
	
	@Override
	public void loadFromDB() {
		
	}
	
	@Override
	public String getHash() {
		return "1";
	}

	public DBTable getTable() {
		return table;
	}

	public void setTable(DBTable table) {
		this.table = table;
		name = table.getSchema()+"/"+table.getName()+"."+getType().getValue();
	}

	public Map<String, DBTableField> getFields() {
		return fields;
	}

	public void setFields(Map<String, DBTableField> fields) {
		this.fields = fields;
	}

	public Map<String, DBIndex> getIndexes() {
		return indexes;
	}

	public void setIndexes(Map<String, DBIndex> indexes) {
		this.indexes = indexes;
	}

	public Map<String, DBConstraint> getConstraints() {
		return constraints;
	}

	public void setConstraints(Map<String, DBConstraint> constraints) {
		this.constraints = constraints;
	}
	
	
}
