package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;
import ru.fusionsoft.dbgit.yaml.YamlOrder;

import java.util.Set;

public class DBTable extends DBSchemaObject {

	@YamlOrder(4)
	private String comment;

	public DBTable(String name, StringProperties options, String schema, String owner, Set<String> dependencies, String comment) {
		super(name, options, schema, owner, dependencies);
		this.comment = comment;
	}


	public String getHash() {
		CalcHash ch = new CalcHash()/*{
			public CalcHash addData(String str){
				ConsoleWriter.detailsPrintlnRed(str);
				return super.addData(str);
			}
		}*/;
		ch.addData(this.name);
		ch.addData(this.schema);
		ch.addData(this.owner);
		ch.addData(this.options.toString());
		ch.addData(this.comment);
		return ch.calcHashStr();
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getComment() {
		return this.comment;
	}

}
