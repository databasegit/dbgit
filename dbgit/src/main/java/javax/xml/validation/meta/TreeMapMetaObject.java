package javax.xml.validation.meta;

import java.util.Comparator;
import java.util.TreeMap;

public class TreeMapMetaObject extends TreeMap<String, IMetaObject> implements IMapMetaObject {

	private static final long serialVersionUID = -1939887173598208816L;
	
	public TreeMapMetaObject() {
		super(
			(Comparator<String>) (nm1, nm2) -> TreeMapMetaObject.compare(nm1, nm2)
		);				
	}
	
	private static int compare(String nm1, String nm2) {
		
		//тут порядок объектов -
		/*
		 seq, tbl, fnc, prc, pkg, vw
		 * */
		
		return 0;
	}
	

}
