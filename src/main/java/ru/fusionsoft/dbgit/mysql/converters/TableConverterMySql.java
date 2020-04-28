package ru.fusionsoft.dbgit.mysql.converters;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
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

public class TableConverterMySql implements IDBConvertAdapter {
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
            switch(objDbType) {
                case POSTGRES:
                    //indexes
                    for (DBIndex index : table.getIndexes().values())
                        index.getOptions().get("ddl").setData(indexFromPostgres(index));
                    //constraints
                    for (DBConstraint constraint : table.getConstraints().values())
                        constraint.getOptions().get("ddl").setData((constraintFromPostgres(table, constraint)));
                    break;
                case ORACLE:
                    //indexes
                    for (DBIndex index : table.getIndexes().values())
                        index.getOptions().get("ddl").setData(indexFromOracle(index));
                    //constraints
                    for (DBConstraint constraint : table.getConstraints().values())
                        constraint.getOptions().get("ddl").setData((constraintFromOracle(constraint)));
                    break;
                default:
                    return obj;
            }
        } else {
            throw new ExceptionDBGit("Cannot convert " + obj.getName());
        }
        obj.setDbType(DbType.MYSQL);
        return obj;
    }

    private String indexFromPostgres(DBIndex index) {
        ConsoleWriter.println("Converting table index " + index.getName() + " from postgresql to mysql...");
        return "";
    }

    private String indexFromOracle(DBIndex index) {
        ConsoleWriter.println("Converting table index " + index.getName() + " from oracle to mysql...");
        return "";
    }

    private String constraintFromOracle(DBConstraint constraint) {//TODO: change
        ConsoleWriter.println("Converting table constraint " + constraint.getName() + " from oracle to mysql...");
        Pattern patternConstraint = Pattern.compile("(?<=" + constraint.getName() + ")(.*?)(?=\\))", Pattern.MULTILINE);
        Matcher matcher = patternConstraint.matcher(constraint.getSql());
        if (matcher.find())
            return matcher.group().replace("\"", "") + ")";
        else
            return "";
    }

    private String constraintFromPostgres(MetaTable table, DBConstraint constraint) {//TODO: change
        ConsoleWriter.println("Converting table constraint " + constraint.getName() + " from postgresql to mysql...");

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
        ConsoleWriter.println("Converting table field " + field.getName() + " from " + dbType.toString().toLowerCase() + " to mysql...");
        String result = "";
        switch (field.getTypeUniversal()) {
            case STRING:
                result = "VARCHAR(" + field.getLength() + ")";
                break;
            case NUMBER:
                result = "INT";
                break;
            case DATE:
                result = "TIMESTAMP";
                break;
            case BINARY:
                result = "BLOB";
                break;
            case TEXT:
                result = "TEXT";
                break;
            case BOOLEAN:
                result = "BOOLEAN";
                break;
            case NATIVE:
            default:
                result = "blob";
        }
        return result;
    }
}
