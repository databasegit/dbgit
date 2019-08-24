package ru.fusionsoft.dbgit.oracle.converters;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class TableConverterOracle implements IDBConvertAdapter {

	@Override
	public IMetaObject convert(String dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {

		if (dbType.equals(obj.getDbType()))
			return obj;
		
		if (obj instanceof MetaTable) {
			
			MetaTable table = (MetaTable) obj;			
			
			ConsoleWriter.println("Processing table " + table.getName());
					
			//types
			for (DBTableField field : table.getFields().values()) {
				if (obj.getDbType().equals(IFactoryDBConvertAdapter.POSTGRES))
					field.setTypeSQL(typeFromPostgres(field));
			}
			
			//indexes
			for (DBIndex index : table.getIndexes().values()) {
				if (obj.getDbType().equals(IFactoryDBConvertAdapter.POSTGRES))	
					index.getOptions().get("ddl").setData(indexFromPostgres(index));				
			}

			//constraints
			for (DBConstraint constraint : table.getConstraints().values()) {
				if (obj.getDbType().equals(IFactoryDBConvertAdapter.POSTGRES))
					constraint.getOptions().get("ddl").setData((constraintFromPostgres(table, constraint)));
			}
			
		} else {
			throw new ExceptionDBGit("Cannot convert " + obj.getName());
		}		
		
		obj.setDbType(IFactoryDBConvertAdapter.ORACLE);
		
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
	
	private String typeFromPostgres(DBTableField field) {
		ConsoleWriter.println("Converting table field " + field.getName() + " from postgresql to oracle...");

		String result = "";
		
		switch (field.getTypeUniversal()) {
			case ("string"): {				
				if (field.getFixed())
					result = "CHAR";
				else
					result = "VARCHAR2";
				
				if (field.getLength() == 0)
					field.setLength(4000);
				result += "(" + field.getLength() + ")";
				
				break;
			}
			
			case ("number"): {
				result = "NUMBER";
				break;
			}
			
			case ("date"): {
				result = "TIMESTAMP ";
				break;
			}
			
			case("boolean"): {
				result = "NUMBER";
				break;
			}
			
			case("binary"): {
				result = "BLOB";
				break;
			}
			
			case("text"): {
				result = "CLOB";
				break;
			}
			
			case ("unknown"): {
				result = "native";
				break;
			}
		}
		
		return result;
	}
}
