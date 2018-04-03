package ru.fusionsoft.dbgit.meta;

import java.util.Comparator;
import java.util.TreeMap;

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
		super(
			new Comparator<String>() {
	            @Override
	            public int compare(String nm1, String nm2) {
	                return this.compare(nm1, nm2);
	            }
        });
		
		/*
		 * как в лямбду сунуть ф-цию объекта? Вообщем постарославянски 
		super(
			(Comparator<String>) (nm1, nm2) -> this.compare(nm1, nm2)
		);
		*/

				
	}
	
	protected int compare(String nm1, String nm2) {
		//тут порядок объектов -
		/*
		 seq, tbl, fnc, prc, pkg, vw
		 * */
		//map.get(nm1).getType().getPriority();
		return 0;
	}
	
	

}
