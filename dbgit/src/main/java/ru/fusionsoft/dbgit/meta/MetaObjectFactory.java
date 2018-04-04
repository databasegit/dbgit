package ru.fusionsoft.dbgit.meta;

import java.lang.reflect.Constructor;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;

/**
 * Factory meta class by type 
 * @author mikle
 *
 */
public class MetaObjectFactory  {
	public static IMetaObject createMetaObject(String name) throws ExceptionDBGit {		
		DBGitMetaType tp = DBGitMetaType.valueOf(getExtByName(name));
		
		return createMetaObject(tp);
	}
	
	public static IMetaObject createMetaObject(DBGitMetaType tp) throws ExceptionDBGit {		
		try {		
			Class<?> c = tp.getMetaClass();
			Constructor<?> cons = c.getConstructor(String.class);
			IMetaObject obj = (IMetaObject)cons.newInstance(tp.getValue());
			return obj;
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}	
		
	}
	
	/**
	 * 
	 * @param name
	 * @return Extention file of meta data. This name is string value of DBGitMetaType.
	 */
	public static String getExtByName(String name) {
		//todo 
		return "tbl";
	}
}
