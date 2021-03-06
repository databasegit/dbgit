package ru.fusionsoft.dbgit.meta;

import java.lang.reflect.Constructor;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

/**
 * Factory meta class by type 
 * @author mikle
 *
 */
public class MetaObjectFactory  {
	public static IMetaObject createMetaObject(String name) throws ExceptionDBGit {		
		IDBGitMetaType tp = parseMetaName(name).getType();
		
		if (tp == null) {
			throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "meta", "parseError").withParams(name));
		}
		
		IMetaObject obj = createMetaObject(tp);
		obj.setName(name);
		return obj;
	}
	
	public static IMetaObject createMetaObject(IDBGitMetaType tp) throws ExceptionDBGit {		
		try {		
			Class<?> c = tp.getMetaClass();
			/*
			Constructor<?> cons = c.getConstructor(String.class);
			IMetaObject obj = (IMetaObject)cons.newInstance(tp.getValue());
			*/
			Constructor<?> cons = c.getConstructor();
			IMetaObject obj = (IMetaObject)cons.newInstance();

			obj.setDbType();
			obj.setDbVersion();
			
			return obj;
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}	
		
	}
		
	public static NameMeta parseMetaName(String name)  {
		NameMeta nm = new NameMeta();

		Integer pos = name.lastIndexOf("/");
		if (pos > 0) {
			nm.setSchema(name.substring(0, pos));
		}
		Integer posDot = name.lastIndexOf(".");
		nm.setName(name.substring(pos+1, posDot));
		nm.setType(DBGitMetaType.valueByCode(name.substring(posDot + 1)));

		return nm;

	}
}
