package ru.fusionsoft.dbgit.data_table;

import java.util.Comparator;
import java.util.TreeMap;


public class TreeMapRowData extends TreeMap<String, RowData> implements IMapRowData {

	private static final long serialVersionUID = -1983814811507633737L;

	public TreeMapRowData() {
		super(
			(Comparator<String>) (nm1, nm2) -> compareRowData(nm1, nm2)
		);
	}
	
	@Override
	public IMapRowData put(RowData obj) {
		put(obj.getKey(), obj);
		return this;
	}
	
	public static int compareRowData(String nm1, String nm2) {
		return nm1.compareTo(nm2);
	}

}
