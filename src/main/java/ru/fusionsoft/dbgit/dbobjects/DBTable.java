package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBTable extends DBSchemaObject {
	private StringProperties options = new StringProperties();
	private String comment = "";

	public DBTable() {
		super();
	}

	public DBTable(String name) {
		super();
		this.name = name;
	}

	public StringProperties getOptions() {
		return options;
	}


	public void setOptions(StringProperties options) {
		this.options = options;
	}

	public String getHash() {
		CalcHash ch = new CalcHash()/*{
			public CalcHash addData(String str){
				ConsoleWriter.detailsPrintlnRed(str);
				return super.addData(str);
			}
		}*/;
		ch.addData(this.getName());
		ch.addData(this.getOptions().toString().replaceAll("\\s+", ""));

		return ch.calcHashStr();
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getComment() {
		return this.comment;
	}

}
