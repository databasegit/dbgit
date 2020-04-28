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
	
	public MetaProcedure(DBProcedure pr) throws ExceptionDBGit {
		super(pr);
	}
	
	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DbGitProcedure;
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getFileName(){
		String res = name.replace(".prc", "");

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

		res = res + ".prc";

		return res;
	}

	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		NameMeta nm = MetaObjectFactory.parseMetaName(name);
		
		DBProcedure pr = adapter.getProcedure(nm.getSchema(), nm.getName());
		
		if (pr == null) 
			return false;
		else {
			setSqlObject(pr);
			return true;
		}
	}


}
