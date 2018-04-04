package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.dbobjects.DBSchema;

public class MetaSchema extends MetaObjOptions {
	public MetaSchema() {
		super();
	}
	
	public MetaSchema(DBSchema shema) {
		super(shema);
	}
	
	@Override
	public DBGitMetaType getType() {		
		return DBGitMetaType.DBGitSchema;
	}
	
	@Override
	public void loadFromDB() {
		// load data shema by name

	}
}
