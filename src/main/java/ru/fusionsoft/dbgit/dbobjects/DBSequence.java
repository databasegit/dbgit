package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBSequence extends DBSchemaObject {
	protected Long value;
	private StringProperties options = new StringProperties();

	public DBSequence() { }

	public DBSequence(String name) { this.name = name; }

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}
	
	public StringProperties getOptions() {
		return options;
	}

	private StringProperties getOptionsFiltered() {
		StringProperties props = new StringProperties(getOptions().getData());
		props.setChildren(getOptions().getChildren());
		props.deleteChild("blocking_table");
		return props;
	}
	
	public void setOptions(StringProperties options) {
		this.options = options;
	}
	
	@Override
	public String getHash() {

		CalcHash ch = new CalcHash();
		ch.addData(getSchema());
		ch.addData(getName());
		ch.addData(getOptionsFiltered().toString());
		ch.addData(getValue().toString());
		
		return ch.calcHashStr();		
	}
}
