package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBFunction;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBView;

public class MetaFunction extends MetaSql {
	private String owner;
	private String arguments;
	public MetaFunction() {
		super();
	}
	
	public MetaFunction(DBFunction fun) {
		super(fun);
	}
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public String getArguments() {
		return arguments;
	}
	public void setArguments(String arguments) {
		this.arguments = arguments;
	}
	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DbGitFunction;
	}
	
	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		NameMeta nm = MetaObjectFactory.parseMetaName(getName());
		
		DBFunction fun = adapter.getFunction(nm.getSchema(), nm.getName());
		setSqlObject(fun);
		
		return true;
	}

}
