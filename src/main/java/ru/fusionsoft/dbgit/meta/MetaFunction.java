package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBFunction;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBView;

public class MetaFunction extends MetaSql {
	public MetaFunction() {
		super();
	}
	
	public MetaFunction(DBFunction fun) throws ExceptionDBGit {
		super(fun);
	}

	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DbGitFunction;
	}

	@Override
	public String getName() {
		String res = name.replace(".fnc", "");
		
		if (this.getSqlObject() != null && this.getSqlObject().getOptions() != null && this.getSqlObject().getOptions().get("arguments") != null)
			res = res + "_" + this.getSqlObject().getOptions().get("arguments").getData()
				.replace(" ", "_")
				.replace("[", "")
				.replace("]", "")
				.replace("\\", "_")
				.replace(",", "");
		
		res = res + ".fnc";
		
		res = res.replace("_.fnc", ".fnc");
		
		return res;
	}
	
	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		NameMeta nm = MetaObjectFactory.parseMetaName(getName());
		
		DBFunction fun = adapter.getFunction(nm.getSchema(), nm.getName());
		if (fun != null) {
			setSqlObject(fun);
			return true;
		}
		else
			return false;
	}

}
