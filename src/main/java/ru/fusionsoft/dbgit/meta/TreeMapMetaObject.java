package ru.fusionsoft.dbgit.meta;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

/**
 * Order map for IMapMetaObject
 * compare function use priority type of IMapMetaObject
 *
 * @author mikle
 *
 */
public class TreeMapMetaObject extends TreeMap<String, IMetaObject> implements IMapMetaObject {

	private static final long serialVersionUID = -1939887173598208816L;

	public TreeMapMetaObject() {
		/*
		super(
			new Comparator<String>() {
	            @Override
	            public int compare(String nm1, String nm2) {
	                return compareMeta(nm1, nm2);
	            }
        });
		*/

		// как в лямбду сунуть ф-цию объекта? Вообщем постарославянски
		super(
				(Comparator<String>) (nm1, nm2) -> compareMeta(nm1, nm2)
		);

	}

	public TreeMapMetaObject(List<IMetaObject> from){
		this();
		this.putAll(from.stream().collect(Collectors.toMap(IMetaObject::getName, key->key)));
	}

	@Override
	public IMapMetaObject put(IMetaObject obj) {
		put(obj.getName(), obj);
		return this;
	}

	@Override
	public SortedListMetaObject getSortedList() {
		return new SortedListMetaObject(this.values());
	}


	public static int compareMeta(String nm1, String nm2) {
		//тут порядок объектов
		try {
			NameMeta obj1 = MetaObjectFactory.parseMetaName(nm1);
			NameMeta obj2 = MetaObjectFactory.parseMetaName(nm2);

			if (obj1.getType() == null ) return -1;
			if (obj2.getType() == null ) return 1;

			int comparePriority = obj1.getType().getPriority() - obj2.getType().getPriority();

			if (comparePriority != 0) {
				return comparePriority;
			}

			return nm1.compareTo(nm2);
		} catch (Exception e) {
			LoggerUtil.getGlobalLogger().error(DBGitLang.getInstance().getValue("errors", "meta", "compareMetaError").withParams(nm1, nm2), e);
			return 0;
		}
	}
}
