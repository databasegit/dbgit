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
		return name;
	}

	@Override
	public String getFileName(){
		String res = name.replace(".fnc", "");

		if (this.getSqlObject() != null && this.getSqlObject().getOptions() != null && this.getSqlObject().getOptions().get("arguments") != null)
			res = res + "_" + this.getSqlObject().getOptions().get("arguments").getData()
					.replace(" ", "_")
					.replace("[", "")
					.replace("]", "")
					.replace("\\", "_")
					.replace(",", "")
					.replace("\"", "")
					.replace("::", "");

		if (res.endsWith("_")) res = res.substring(0, res.length() - 1);
		if (res.length() > MAX_FILE_NAME_LENGTH) {
			String resTemp = res.substring(0, MAX_FILE_NAME_LENGTH);
			int resInt = res.length() - MAX_FILE_NAME_LENGTH;
			res = resTemp + "_" + resInt;
		}

		res = res + ".fnc";

		return res;
	}
	
	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		NameMeta nm = MetaObjectFactory.parseMetaName(name);
		
		DBFunction fun = adapter.getFunction(nm.getSchema(), nm.getName());
		if (fun != null) {
			setSqlObject(fun);
			return true;
		}
		else
			return false;
	}

}
