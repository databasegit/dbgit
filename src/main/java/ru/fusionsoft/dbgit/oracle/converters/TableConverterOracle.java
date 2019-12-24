package ru.fusionsoft.dbgit.oracle.converters;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class TableConverterOracle implements IDBConvertAdapter {

	@Override
	public IMetaObject convert(DbType dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {
		DbType objDbType = obj.getDbType();
		if (dbType == objDbType)
			return obj;
		
		if (obj instanceof MetaTable) {
			
			MetaTable table = (MetaTable) obj;			
			
			ConsoleWriter.println("Processing table " + table.getName());
					
			//types
			for (DBTableField field : table.getFields().values())
				field.setTypeSQL(typeFromAnotherDB(objDbType, field));
			
			//indexes
			for (DBIndex index : table.getIndexes().values()) {
				if (objDbType == DbType.POSTGRES)
					index.getOptions().get("ddl").setData(indexFromPostgres(index));				
			}

			//constraints
			for (DBConstraint constraint : table.getConstraints().values()) {
				if (objDbType == DbType.POSTGRES)
					constraint.getOptions().get("ddl").setData((constraintFromPostgres(table, constraint)));
			}
			
		} else {
			throw new ExceptionDBGit("Cannot convert " + obj.getName());
		}		
		
		obj.setDbType(DbType.ORACLE);
		
		return obj;
	}

	private String indexFromPostgres(DBIndex index) {
		ConsoleWriter.println("Converting table index " + index.getName() + " from postgresql to oracle...");
		
		return "";
	}
	
	private String constraintFromPostgres(MetaTable table, DBConstraint constraint) {
		ConsoleWriter.println("Converting table constraint " + constraint.getName() + " from postgresql to oracle...");
		
		String ddl = constraint.getOptions().get("ddl")
				.toString()
				.replace("ON UPDATE CASCADE", "")
				.replace("ON DELETE CASCADE", "")
				.replace("MATCH FULL", "");
		
		if (!ddl.contains("."))
			ddl = ddl.replace("REFERENCES ", "REFERENCES " + table.getTable().getSchema() + ".");
		
		return "alter table " + table.getTable().getSchema() + "." + table.getTable().getName() + 
				" add constraint " + constraint.getName() + " " + ddl;
	}
	
	private String typeFromAnotherDB(DbType dbType, DBTableField field) {
		ConsoleWriter.println("Converting table field " + field.getName() + " from " + dbType.toString().toLowerCase() + " to oracle...");
		String result = "";
		switch (field.getTypeUniversal()) {
			case STRING:
				if (field.getFixed())
					result = "CHAR";
				else
					result = "VARCHAR2";
				
				if (field.getLength() == 0)
					field.setLength(4000);
				result += "(" + field.getLength() + ")";
				break;
			case NUMBER:
				result = "NUMBER";
				break;
			case DATE:
				result = "TIMESTAMP ";
				break;
			case BOOLEAN:
				result = "NUMBER";
				break;
			case BINARY:
				result = "BLOB";
				break;
			case TEXT:
				result = "CLOB";
				break;
			case NATIVE:
				result = "native";
				break;
		}
		return result;
	}
}
