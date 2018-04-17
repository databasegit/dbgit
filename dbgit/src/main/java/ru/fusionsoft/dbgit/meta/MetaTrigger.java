package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
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
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		NameMeta nm = MetaObjectFactory.parseMetaName(getName());
		
		DBTrigger trg = adapter.getTrigger(nm.getSchema(), nm.getName());
		setSqlObject(trg);
		
		return true;
	}
}
