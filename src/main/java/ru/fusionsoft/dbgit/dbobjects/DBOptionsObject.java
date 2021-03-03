package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;
import ru.fusionsoft.dbgit.yaml.YamlOrder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DBOptionsObject implements IDBObject {

	@YamlOrder(0)
	String name;

	@YamlOrder(99)
	StringProperties options;

	public DBOptionsObject(String name) {
		this.name = name;
		this.options = new StringProperties();
	}
	public DBOptionsObject(String name, StringProperties options) {
		this.name = name;
		this.options = options;
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof DBOptionsObject)) return false;

		DBOptionsObject that = (DBOptionsObject) o;
		return this.getHash().equals(that.getHash());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getHash());
	}

	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(this.name);
		ch.addData(this.options.toString());

		return ch.calcHashStr();
	}

	public StringProperties getOptions() {
		return options;
	}
	public void setOptions(StringProperties opt) {
		options = opt;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}


}
