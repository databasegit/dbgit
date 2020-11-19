package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.yaml.YamlOrder;

/**
 * Meta class for db Table
 * @author mikle
 *
 */
public class MetaTable extends MetaBase {

	@YamlOrder(1)
	private DBTable table;

	@YamlOrder(2)
	//private IMapFields fields = new TreeMapFields();
	private Map<String, DBTableField> fields = new TreeMap<>();

	@YamlOrder(3)
	private Map<String, DBIndex> indexes = new TreeMap<>();

	@YamlOrder(4)
	private Map<String, DBConstraint> constraints = new TreeMap<>();

	public MetaTable() {
		setDbType();
		setDbVersion();
	}

	public MetaTable(String namePath) {
		setDbType();
		setDbVersion();

		this.name = namePath;
	}

	public MetaTable(DBTable tbl) {
		setDbType();
		setDbVersion();
		setTable(tbl);
	}

	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DBGitTable;
	}
	
	@Override
	public boolean serialize(OutputStream stream) throws IOException {
		return yamlSerialize(stream);
	}

	@Override
	public IMetaObject deSerialize(InputStream stream) throws IOException {
		return yamlDeSerialize(stream);
	}
	
	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		NameMeta nm = MetaObjectFactory.parseMetaName(getName());
		
		DBTable tbl = adapter.getTable(nm.getSchema(), nm.getName());
		if (tbl != null)
			return loadFromDB(tbl);
		else
			return false;
	}
	
	public boolean loadFromDB(DBTable tbl) throws ExceptionDBGit {
		setTable(tbl);
		
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		Map<String, DBTableField> actualFields = adapter.getTableFields(tbl.getSchema(), tbl.getName());
		
		if (fields.size() == 0) {
			fields.putAll(actualFields);
		}

		if (!fields.keySet().equals(actualFields.keySet())) {		
			fields.clear();
			fields.putAll(actualFields);
			
			if (DBGitIndex.getInctance().getItemIndex(tbl.getSchema() + "/" + tbl.getName() + ".csv") != null) {
				MetaTableData tableData = new MetaTableData(tbl);
				
				tableData.loadFromDB();
				tableData.saveToFile();
			}
			
		} else {		
			fields.putAll(actualFields);
		}
		if (indexes.size() == 0)
			indexes.putAll(adapter.getIndexes(tbl.getSchema(), tbl.getName()));
		
		if (constraints.size() == 0)
			constraints.putAll(adapter.getConstraints(tbl.getSchema(), tbl.getName()));
		/*
		if (dependencies.size() == 0)
			dependencies.addAll(adapter.getDependencies(tbl.getSchema(), tbl.getName()));
		 */
		return true;
	}
	
	@Override
	public String getHash() {
		CalcHash ch = new CalcHash()/*{
			@Override
			public CalcHash addData(String str){
				ConsoleWriter.printlnRed(str);
				return super.addData(str);
			}
		}*/;
		ch.addData(this.getName());
		
		if (getTable() != null) {
			ch.addData(this.getTable().getHash());
		}
		
		if (fields == null)
			return EMPTY_HASH;
		
		for (String item : fields.keySet()) {
			ch.addData(item);
			ch.addData(fields.get(item).getHash());

		}
		
		for (String item : indexes.keySet()) {
			if(constraints.containsKey(item)) continue;
			ch.addData(item);
			ch.addData(indexes.get(item).getHash());

		}

		for (String item : constraints.keySet()) {
			ch.addData(item);
			ch.addData(constraints.get(item).getHash());

		}

//		if(this.getTable() != null && this.getTable().getName().contains("clients")){
//			ConsoleWriter.printlnRed(MessageFormat.format("-\t-\t-\t-\t-\t-\ntable = {0} ; {1}, \nfields({2}) = \n{3}\nindexes({4}) = \n{5}\nconstraints({6}) = \n{7}"
//				,this.getTable() != null ? this.getTable().getName() + " ; " + truncateHash(this.getTable().getHash()) : "noname"
//				,this.getTable() != null ? this.getTable().getOptions().getChildren().entrySet().stream().map(x->"\n\t\t" + x.getKey() + " : " + x.getValue().getData()).collect(Collectors.joining("")) : "null"
//				,fields.keySet().size()
//				,fields.values().stream()
//					.map(x->"\t\t" + x.getName() + ";" + x.getDefaultValue())
//					.collect(Collectors.joining("\n"))
//				,indexes.keySet().size()
//				,indexes.values().stream()
//					.map(x->"\t\t" + x.getName() + ";" + truncateHash(x.getHash()))
//					.collect(Collectors.joining("\n"))
//				,constraints.keySet().size()
//				,constraints.values().stream()
//					.map(x->"\t\t" + x.getName() + ";" + truncateHash(x.getHash()))
//					.collect(Collectors.joining("\n"))
//			));
//		}
		return ch.calcHashStr();		
	}

	public DBTable getTable() {
		return table;
	}

	public void setTable(DBTable table) {
		this.table = table;
		name = table.getSchema()+"/"+table.getName()+"."+getType().getValue();
	}

	/**
	 * return sorted map. Not use for add element. See function getFieldsMap
	 * @return
	 */
	public Map<String, DBTableField> getFields() {
		return  
				fields.entrySet().stream()
			    .sorted(Entry.comparingByValue())
			    .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
			                              (e1, e2) -> e1, LinkedHashMap::new));
	}
	
	/**
	 * Return map fields. This map can be used for add/edit/delete elements
	 * @return
	 */
	public Map<String, DBTableField> getFieldsMap() {
		return fields;
	}

	public void setFields(Map<String, DBTableField> fields) {
		this.fields.clear();
		this.fields.putAll(fields);
	}

	public Map<String, DBIndex> getIndexes() {
		return indexes;
	}

	public void setIndexes(Map<String, DBIndex> indexes) {
		this.indexes.clear(); 
		this.indexes.putAll(indexes);
	}

	public Map<String, DBConstraint> getConstraints() {
		return constraints;
	}

	public void setConstraints(Map<String, DBConstraint> constraints) {
		this.constraints.clear(); 
		this.constraints.putAll(constraints);
	}
	
	public List<Integer> getIdColumns() {
		List<Integer> idColumns = new ArrayList<>();

		int i = 0;
		for (DBTableField field : fields.values()) {
			if (field.getIsPrimaryKey()) {
				//idColumns.add(field.getName());
				idColumns.add(i);
			}
			i++;
		}
		
		return idColumns;
	}
//	private String truncateHash(String hash){
//		return hash.substring(
//				0,
//				2
//		) + hash.substring(
//				hash.length() - 3,
//				hash.length() - 1
//		);
//	}
}
