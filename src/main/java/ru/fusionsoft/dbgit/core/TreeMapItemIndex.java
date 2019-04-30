package ru.fusionsoft.dbgit.core;

import java.util.Comparator;
import java.util.TreeMap;

import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;


public class TreeMapItemIndex extends TreeMap<String, ItemIndex> implements IMapItemIndex {
	private static final long serialVersionUID = -6115513868752264705L;

	public TreeMapItemIndex() {
		super(
			(Comparator<String>) (nm1, nm2) -> TreeMapMetaObject.compareMeta(nm1, nm2)
		);
	}
}
