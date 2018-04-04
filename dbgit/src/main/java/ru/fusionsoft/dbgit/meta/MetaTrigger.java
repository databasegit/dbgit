package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.dbobjects.DBTrigger;

public class MetaTrigger extends MetaSql {
	public MetaTrigger() {
		super();
	}
	
	public MetaTrigger(DBTrigger pr) {
		super(pr);
	}
	
	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DbGitTrigger;
	}
	
	@Override
	public void loadFromDB() {
		// load data shema by name

	}


}
