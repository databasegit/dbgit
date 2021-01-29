package ru.fusionsoft.dbgit.postgres.converters;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class TableConverterPostgresql implements IDBConvertAdapter {

	@Override
	public IMetaObject convert(DbType dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {
		DbType objDbType = obj.getDbType();
		if (dbType == objDbType)
			return obj;
		
		if (obj instanceof MetaTable) {
			
			MetaTable table = (MetaTable) obj;
			ConsoleWriter.println(DBGitLang.getInstance()
			    .getValue("general", "convert", "processingTable")
			    .withParams(table.getName())
			    , messageLevel
			);

			//types
			for (DBTableField field : table.getFields().values())
				field.setTypeSQL(typeFromAnotherDB(objDbType, field));
			
			//indexes
			for (DBIndex index : table.getIndexes().values()) {
				if (objDbType == DbType.ORACLE)
					index.getOptions().get("ddl").setData(indexFromOracle(index));				
			}

			//constraints
			for (DBConstraint constraint : table.getConstraints().values()) {
				if (objDbType == DbType.ORACLE)
					constraint.getOptions().get("ddl").setData(constraintFromOracle(constraint));
			}
			
		} else {
			throw new ExceptionDBGit("Cannot convert " + obj.getName());
		}		
		
		obj.setDbType(DbType.POSTGRES);
		
		return obj;
	}

	private String indexFromOracle(DBIndex index) {
		ConsoleWriter.println(DBGitLang.getInstance()
		    .getValue("general", "convert", "convertingIndex")
		    .withParams(index.getName(), "oracle", "postgresql")
		    , messageLevel
		);

		return "";
	}
	
	private String constraintFromOracle(DBConstraint constraint) {
		ConsoleWriter.println(DBGitLang.getInstance()
			.getValue("general", "convert", "convertingConstraint")
			.withParams(constraint.getName(), "oracle", "postgresql")
			, messageLevel
		);

		Pattern patternConstraint = Pattern.compile("(?<=" + constraint.getName() + ")(.*?)(?=\\))", Pattern.MULTILINE);
		Matcher matcher = patternConstraint.matcher(constraint.getSql());

		if (matcher.find()) {
			return matcher.group().replace("\"", "") + ")";
		} else {
			return "";
		}
	}
	
	private String typeFromAnotherDB(DbType dbType, DBTableField field) {
		ConsoleWriter.println(DBGitLang.getInstance()
			.getValue("general", "convert", "convertingField")
			.withParams(field.getName(), dbType.toString(), "postgresql")
			, messageLevel
		);
		String result = "";
		switch (field.getTypeUniversal()) {
			case STRING:
				if (field.getFixed())
					result = "character";
				else
					result = "character varying";

				if (field.getLength() > 0)
					result += "(" + field.getLength() + ")";
				break;
			case NUMBER:
				result = "numeric";
				break;
			case DATE:
				result = "timestamp without time zone";
				break;
			case BINARY:
				result = "bytea";
				break;
			case TEXT:
				result = "text";
				break;
			case NATIVE:
				result = "native";
				break;
		}
		return result;
	}
}
