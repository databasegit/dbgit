package ru.fusionsoft.dbgit.meta;

import java.util.Map;

/**
 * Interface Map Meta Object
 * 
 * @author mikle
 *
 */
public interface IMapMetaObject extends Map<String, IMetaObject> {
	public IMapMetaObject put(IMetaObject obj);
}
