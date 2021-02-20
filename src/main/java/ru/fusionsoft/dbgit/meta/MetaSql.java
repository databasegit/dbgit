package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;

/**
 * Meta class for sql data
 * @author mikle
 *
 */
public abstract class MetaSql extends MetaBase {


	protected DBSQLObject sqlObject;
	public MetaSql() {
		setDbType();
		setDbVersion();
	}

	public MetaSql(DBSQLObject sqlObject) throws ExceptionDBGit {
		this();
		setSqlObject(sqlObject);
	}

	public DBSQLObject getSqlObject() {
		return sqlObject;
	}


	public void setSqlObject(DBSQLObject sqlObject) throws ExceptionDBGit {
		this.sqlObject = sqlObject;
		setName(sqlObject.getSchema()+"/"+sqlObject.getName()+"."+getType().getValue());
	}

	@Override
	public boolean serialize(OutputStream stream) throws IOException {
		/*
		String owner = "owner: "+getSqlObject().getOwner()+"\n";
		stream.write(owner.getBytes(Charset.forName("UTF-8")));
		*/
		
		/*
		stream.write(getSqlObject().getSql().getBytes(Charset.forName("UTF-8")));
		return true;
		*/
		return yamlSerialize(stream);
	}

	@Override
	public IMetaObject deSerialize(InputStream stream)  {
		NameMeta nm = MetaObjectFactory.parseMetaName(getName());
		/*
		sqlObject = new DBSQLObject();		
		sqlObject.setName(nm.getName());
		sqlObject.setSchema(nm.getSchema());
		

		sqlObject.setSql(IOUtils.toString(stream, StandardCharsets.UTF_8.name()));
		
		return this;
		*/
		/*
		MetaSql obj = (MetaSql)yamlDeSerialize(stream);
		obj.sqlObject.setName(nm.getName());
		obj.sqlObject.setSchema(nm.getSchema());
		*/
		return yamlDeSerialize(stream);
	}

	@Override
	public String getHash() {
		return sqlObject != null ? sqlObject.getHash() : EMPTY_HASH;
	}

//	public void setSqlObjectFromMap(Map<String, ? extends DBSQLObject> map) throws ExceptionDBGit {
//		NameMeta nm = MetaObjectFactory.parseMetaName(getName());
//		if (!map.containsKey(nm.getName())) {
//			throw new ExceptionDBGitObjectNotFound(DBGitLang.getInstance().getValue("errors", "meta", "notFound").withParams(getName()));
//		}
//		setSqlObject(map.get(nm.getName()));
//	}

}
