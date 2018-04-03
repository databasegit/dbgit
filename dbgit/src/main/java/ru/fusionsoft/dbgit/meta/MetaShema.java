package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.dbobjects.DBSchema;

public class MetaShema extends MetaObjOptions {
	public MetaShema() {
		setType(DBGitMetaType.DBGitSchema);
	}
	
	public MetaShema(DBSchema shema) {
		this();
		setObjectOption(shema);		
	}
	
	@Override
	public void loadFromDB() {
		// load data shema by name

	}
}
