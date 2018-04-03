package ru.fusionsoft.dbgit.meta;

import java.lang.reflect.Constructor;

/**
 * Factory meta class by type 
 * @author mikle
 *
 */
public class MetaObjectFactory  {
	public static IMetaObject createMetaObject(String name) throws Exception {		
		DBGitMetaType tp = DBGitMetaType.valueOf(getExtByName(name));
		
		return createMetaObject(tp);
	}
	
	public static IMetaObject createMetaObject(DBGitMetaType tp) throws Exception {		
				
		Class<?> c = tp.getMetaClass();
		Constructor<?> cons = c.getConstructor(String.class);
		IMetaObject obj = (IMetaObject)cons.newInstance(tp.getValue());
				
		return obj;
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
