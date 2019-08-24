package ru.fusionsoft.dbgit.postgres.converters;

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

public class TableConverterPostgresql implements IDBConvertAdapter {

	@Override
	public IMetaObject convert(String dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {

		if (dbType.equals(obj.getDbType()))
			return obj;
		
		if (obj instanceof MetaTable) {
			
			MetaTable table = (MetaTable) obj;
			
			ConsoleWriter.println("Processing table " + table.getName());
					
			//types
			for (DBTableField field : table.getFields().values()) {
				if (obj.getDbType().equals(IFactoryDBConvertAdapter.ORACLE))
					field.setTypeSQL(typeFromOracle(field));
			}
			
			//indexes
			for (DBIndex index : table.getIndexes().values()) {
				if (obj.getDbType().equals(IFactoryDBConvertAdapter.ORACLE))	
					index.getOptions().get("ddl").setData(indexFromOracle(index));				
			}

			//constraints
			for (DBConstraint constraint : table.getConstraints().values()) {
				if (obj.getDbType().equals(IFactoryDBConvertAdapter.ORACLE))
					constraint.getOptions().get("ddl").setData(constraintFromOracle(constraint));
			}
			
		} else {
			throw new ExceptionDBGit("Cannot convert " + obj.getName());
		}		
		
		obj.setDbType(IFactoryDBConvertAdapter.POSTGRES);
		
		return obj;
	}

	private String indexFromOracle(DBIndex index) {
		ConsoleWriter.println("Converting table index " + index.getName() + " from oracle to postgresql...");
		
		return "";
	}
	
	private String constraintFromOracle(DBConstraint constraint) {
		ConsoleWriter.println("Converting table constraint " + constraint.getName() + " from oracle to postgresql...");
		
			Pattern patternConstraint = Pattern.compile("(?<=" + constraint.getName() + ")(.*?)(?=\\))", Pattern.MULTILINE);
			Matcher matcher = patternConstraint.matcher(constraint.getSql());
	
			if (matcher.find()) {
				return matcher.group().replace("\"", "") + ")";				
			} else {
				return "";
			}
	}
	
	private String typeFromOracle(DBTableField field) {
		ConsoleWriter.println("Converting table field " + field.getName() + " from oracle to postgresql...");

		String result = "";
		
		switch (field.getTypeUniversal()) {
			case ("string"): {				
				if (field.getFixed())
					result = "character";
				else
					result = "character varying";
				if (field.getLength() > 0)
					result += "(" + field.getLength() + ")";
				
				break;
			}
			
			case ("number"): {
				result = "numeric";
				break;
			}
			
			case ("date"): {
				result = "timestamp without time zone";
				break;
			}
			
			case("binary"): {
				result = "bytea";
				break;
			}
			
			case("text"): {
				result = "text";
				break;
			}
			
			case ("unknown"): {
				result = "native";
				break;
			}
			
			default: {
				result = "def_" + field.getTypeUniversal();
				break;
			}

		}
		
		return result;
	}
}
