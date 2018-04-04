package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.dbobjects.DBView;

public class MetaView  extends MetaSql {
	public MetaView() {
		super();
	}
	
	public MetaView(DBView vw) {
		super(vw);
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
