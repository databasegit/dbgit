package ru.fusionsoft.dbgit.meta;

import java.util.Map;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
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
	public void loadFromDB() throws ExceptionDBGit {
		// load data shema by name
		IDBAdapter adapter = AdapterFactory.createAdapter();
		Map<String, DBSchema> schemes = adapter.getSchemes();
		
		setObjectOptionFromMap(schemes);
	}
}
