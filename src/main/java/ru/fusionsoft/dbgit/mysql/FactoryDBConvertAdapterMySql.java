package ru.fusionsoft.dbgit.mysql;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.mysql.converters.*;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FactoryDBConvertAdapterMySql implements IFactoryDBConvertAdapter {
    private static final Map<String, IDBConvertAdapter> converters;

    static {
        Map<String, IDBConvertAdapter> aMap = new HashMap<String, IDBConvertAdapter>();
        aMap.put(DBGitMetaType.DBGitTable.getValue(), new TableConverterMySql());
        aMap.put(DBGitMetaType.DbGitTableData.getValue(), new TableDataConverterMySql());
        //aMap.put(DBGitMetaType.DbGitFunction.getValue(), new FunctionConverterMySql());
        aMap.put(DBGitMetaType.DbGitView.getValue(), new ViewConverterMySql());

        converters = Collections.unmodifiableMap(aMap);
    }

    @Override
    public IDBConvertAdapter getConvertAdapter(String objectType) throws Exception {
        if (!converters.containsKey(objectType)) {
            ConsoleWriter.println("Cannot convert " + objectType + "!");
            return null;
        } else
            return converters.get(objectType);
    }
}
