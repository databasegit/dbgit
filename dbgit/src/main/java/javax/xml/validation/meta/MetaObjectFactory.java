package javax.xml.validation.meta;

import java.lang.reflect.Constructor;

public class MetaObjectFactory  {
	public static IMetaObject createMetaObject(String name) throws Exception {		
		DBGitMetaType tp = DBGitMetaType.valueOf(name);
		
		Class<?> c = tp.getMetaClass();
		Constructor<?> cons = c.getConstructor(String.class);
		IMetaObject obj = (IMetaObject)cons.newInstance(name);
				
		return obj;
	}
	
	public static String getExtByName(String name) {
		return "tbl";
	}
}
