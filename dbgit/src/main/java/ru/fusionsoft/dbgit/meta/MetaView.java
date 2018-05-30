package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
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
		return DBGitMetaType.DbGitView;
	}
	
	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		NameMeta nm = MetaObjectFactory.parseMetaName(getName());
		
		DBView vw = adapter.getView(nm.getSchema(), nm.getName());
		setSqlObject(vw);
		
		return true;
	}

}
