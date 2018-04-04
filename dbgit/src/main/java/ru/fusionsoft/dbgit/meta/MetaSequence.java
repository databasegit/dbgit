package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.dbobjects.DBSequence;

public class MetaSequence extends MetaSql {
	public MetaSequence() {
		super();
	}
	
	public MetaSequence(DBSequence seq) {
		super(seq);
	}
	
	@Override
	public DBGitMetaType getType() {		
		return DBGitMetaType.DBGitSequence;
	}
	
	@Override
	public void loadFromDB() {
		// load data shema by name

	}

}
