package ru.fusionsoft.dbgit.data_table;

import java.util.Map;

public interface IMapRowData  extends Map<String, RowData> {
	public IMapRowData put(RowData obj);

}
