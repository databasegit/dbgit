package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.yaml.YamlOrder;

import java.nio.channels.IllegalSelectorException;
import java.util.Objects;

public class DBTableField implements IDBObject, Comparable<DBTableField> {

	@YamlOrder(0)
	private String name = "";
	@YamlOrder(1)
	private String description = "";
	@YamlOrder(2)
	private Boolean isPrimaryKey = false;
	@YamlOrder(3)
	private Boolean isNullable = false;
	@YamlOrder(4)
	private String typeSQL = "";
	@YamlOrder(5)
	private FieldType typeUniversal = FieldType.UNDEFINED;
	@YamlOrder(6)
	private Integer order = -1;
	@YamlOrder(7)
	private String defaultValue = "";
	@YamlOrder(8)
	private int length = 0;
	@YamlOrder(9)
	private int scale = 0;
	@YamlOrder(10)
	private int precision = 0;
	@YamlOrder(11)
	private boolean fixed = false;

	public DBTableField() {
		//Just for things that want to introspect my guts and use setters
		this.order = -1;
		this.defaultValue = "";
		this.description = "";
		this.fixed = false;
		this.isNullable = false;
		this.isPrimaryKey = false;
		this.length = 0;
		this.name = "";
		this.precision = 0;
		this.scale = 0;
		this.typeSQL = "";
		this.typeUniversal = FieldType.UNDEFINED;
	}

	public DBTableField(String name, String description, Boolean isPrimaryKey, Boolean isNullable, String typeSQL, FieldType typeUniversal, Integer order, String defaultValue, int length, int scale, int precision, boolean fixed) {
		this.name = name;
		this.description = description == null ? "" : description;
		this.isPrimaryKey = isPrimaryKey;
		this.isNullable = isNullable;
		this.typeSQL = typeSQL;
		this.typeUniversal = typeUniversal;
		this.order = order;
		this.defaultValue = defaultValue == null ? "" : defaultValue;
		this.length = length;
		this.scale = scale;
		this.precision = precision;
		this.fixed = fixed;
	}

	@Override
	public int compareTo(DBTableField o) {
		int res = - isPrimaryKey.compareTo(o.getIsPrimaryKey());
		if (res != 0) return res;
		return order.compareTo(o.getOrder());
//		return name.compareTo(o.getName());
	}

	@Override public boolean equals(Object obj){
		boolean equals = obj == this;
		if(!equals && obj instanceof DBTableField){
			return ((DBTableField) obj).getHash().equals(this.getHash());
		}
		return equals;
	}

	@Override
	public int hashCode() {
		return Objects.hash(getHash());
	}

	public String getHash() {
		CalcHash ch = new CalcHash();

		ch.addData(this.name);
		ch.addData(this.typeSQL);
		ch.addData(this.order);
		ch.addData(this.isPrimaryKey);
		ch.addData(this.isNullable);
		ch.addData(this.description);
		ch.addData(this.defaultValue);
		ch.addData(this.fixed);
		ch.addData(this.length);
		ch.addData(this.precision);
		ch.addData(this.scale);

		return ch.calcHashStr();
	}


	public Boolean getIsPrimaryKey() {
		return isPrimaryKey;
	}

	public Boolean getIsNullable() { return isNullable; }

	public FieldType getTypeUniversal() {
		return typeUniversal;
	}

	public int getLength() {
		return length;
	}

	public int getPrecision() {
		return precision;
	}

	public boolean getFixed() {
		return fixed;
	}

	public String getName() {
		return name;
	}

	public String getTypeSQL() {
		return typeSQL;
	}

	public Integer getOrder() {
		return order;
	}

	public String getDefaultValue() {
		return this.defaultValue;
	}

	public int getScale() {
		return scale;
	}

	public String getDescription() {
		return this.description;
	}

	public void setIsPrimaryKey(Boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	public void setTypeUniversal(FieldType typeUniversal) {
		this.typeUniversal = typeUniversal == null ? FieldType.UNDEFINED : typeUniversal;
	}

	public void setIsNullable(Boolean isNullable) { this.isNullable = isNullable; }

	public void setLength(int length) {
		this.length = length;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public void setFixed(boolean fixed) {
		this.fixed = fixed;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setOrder(Integer order) {
		this.order = order;
	}

	public void setTypeSQL(String typeSQL) {
		this.typeSQL = typeSQL;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue == null ? "" : defaultValue;
	}

	public void setDescription(String description) {
		this.description = description == null ? "" : description;
	}


	

}
