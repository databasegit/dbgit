package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBPackage;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;

public class MetaPackage extends MetaSql {
	public MetaPackage() {
		super();
	}
	
	public MetaPackage(DBPackage pac) {
		super(pac);
	}
	
	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DbGitPackage;
	}
	
	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		NameMeta nm = MetaObjectFactory.parseMetaName(getName());

		DBPackage pkg = adapter.getPackage(nm.getSchema(), nm.getName());		
		
		if (pkg == null) 
			return false;
		else {
			setSqlObject(pkg);
			return true;
		}
	}

}
