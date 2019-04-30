package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ItemIndex;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
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
	}
	
	public MetaTable(String namePath) {
		this.name = namePath;
	}
	
	public MetaTable(DBTable tbl) {
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

		indexes.putAll(adapter.getIndexes(tbl.getSchema(), tbl.getName()));
		constraints.putAll(adapter.getConstraints(tbl.getSchema(), tbl.getName()));
		return true;
	}
	
	@Override
	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(this.getName());
		
		if (getTable() != null)
			ch.addData(this.getTable().getHash());
		
		for (String item : fields.keySet()) {
			ch.addData(item);
			ch.addData(fields.get(item).getHash());
		}
		
		for (String item : indexes.keySet()) {
			ch.addData(item);
			ch.addData(indexes.get(item).getHash());
		}
		
		for (String item : constraints.keySet()) {
			ch.addData(item);
			ch.addData(constraints.get(item).getHash());
		}

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
	
	public List<String> getIdColumns() {
		List<String> idColumns = new ArrayList<>();
		
		for (DBTableField field : fields.values()) {
			if (field.getIsPrimaryKey()) {
				idColumns.add(field.getName());
			}
		}
		
		return idColumns;
	}
}
