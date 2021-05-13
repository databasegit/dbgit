package ru.fusionsoft.dbgit.dbobjects;

import java.util.Collections;
import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.util.Set;

public class DBSequence extends DBSchemaObject {
	protected Long value;

	public DBSequence(){
		super("", new StringProperties(), "", "", Collections.emptySet());
	}
	public DBSequence(String name, StringProperties options, String schema, String owner, Set<String> dependencies, Long value) {
		super(name, options, schema, owner, dependencies);
		this.value = value;
	}

	public StringProperties persistentOptions() {
		//Immutable, yeah
		StringProperties props = new StringProperties(getOptions().getData());
		props.setChildren(getOptions().getChildren());
		props.deleteChild("blocking_table");
		return props;
	}

	@Override
	public String getHash() {

		CalcHash ch = new CalcHash();
		ch.addData(this.schema);
		ch.addData(this.name);
		ch.addData(this.owner);
		ch.addData(persistentOptions().toString());
		ch.addData(String.valueOf(this.value));
		
		return ch.calcHashStr();		
	}

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}
}
