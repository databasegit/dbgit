package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBPackage;
import ru.fusionsoft.dbgit.dbobjects.DBProcedure;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;

public class MetaProcedure extends MetaSql {
	public MetaProcedure() {
		super();
	}
	
	public MetaProcedure(DBProcedure pr) {
		super(pr);
	}
	
	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DbGitProcedure;
	}
	
	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		NameMeta nm = MetaObjectFactory.parseMetaName(getName());
		
		DBProcedure pr = adapter.getProcedure(nm.getSchema(), nm.getName());
		setSqlObject(pr);
		
		return true;
	}


}
