package ru.fusionsoft.dbgit.mssql.converters;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableConverterMssql implements IDBConvertAdapter {

	@Override
	public IMetaObject convert(DbType dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {

		/*
		if (dbType.equals(obj.getDbType()))
			return obj;

		if (obj instanceof MetaTable) {

			MetaTable table = (MetaTable) obj;

			ConsoleWriter.println("Processing table " + table.getName());

			//types
			for (DBTableField field : table.getFields().values()) {
				if (obj.getDbType().equals(IFactoryDBConvertAdapter.MSSQL))
					field.setTypeSQL(typeFromOracle(field));
			}

			//indexes
			for (DBIndex index : table.getIndexes().values()) {
				if (obj.getDbType().equals(IFactoryDBConvertAdapter.MSSQL))
					index.getOptions().get("ddl").setData(indexFromOracle(index));
			}

			//constraints
			for (DBConstraint constraint : table.getConstraints().values()) {
				if (obj.getDbType().equals(IFactoryDBConvertAdapter.MSSQL))
					constraint.getOptions().get("ddl").setData(constraintFromOracle(constraint));
			}

		} else {
			throw new ExceptionDBGit("Cannot convert " + obj.getName());
		}

		obj.setDbType(IFactoryDBConvertAdapter.MSSQL);
		*/

		return obj;
	}

}
