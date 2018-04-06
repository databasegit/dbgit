package ru.fusionsoft.dbgit.meta;

import java.lang.reflect.Constructor;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;

/**
 * Factory meta class by type 
 * @author mikle
 *
 */
public class MetaObjectFactory  {
	public static IMetaObject createMetaObject(String name) throws ExceptionDBGit {		
		DBGitMetaType tp = parseMetaName(name).getType();
		
		IMetaObject obj = createMetaObject(tp);
		obj.setName(name);
		return obj;
	}
	
	public static IMetaObject createMetaObject(DBGitMetaType tp) throws ExceptionDBGit {		
		try {		
			Class<?> c = tp.getMetaClass();
			/*
			Constructor<?> cons = c.getConstructor(String.class);
			IMetaObject obj = (IMetaObject)cons.newInstance(tp.getValue());
			*/
			Constructor<?> cons = c.getConstructor();
			IMetaObject obj = (IMetaObject)cons.newInstance();
			return obj;
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}	
		
	}
		
	public static NameMeta parseMetaName(String name) {
		try {
			NameMeta nm = new NameMeta();
			
			Integer pos = name.lastIndexOf("/");
			if (pos > 0) {
				nm.setSchema(name.substring(0, pos));
			}
			
			String tmp = name.substring(pos+1);
			String[] partName = tmp.split("\\.");
			nm.setName(partName[0]);
			nm.setType(DBGitMetaType.valueByCode(partName[1]));

			return nm;
		} catch(Exception e) {
			throw new ExceptionDBGitRunTime("Error parse object name: "+name, e);
		}
	}
}
