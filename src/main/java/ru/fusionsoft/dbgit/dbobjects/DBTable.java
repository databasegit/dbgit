package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.core.NotImplementedExceptionDBGitRuntime;
import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;
import ru.fusionsoft.dbgit.yaml.YamlOrder;

import java.util.Collections;
import java.util.Set;

public class DBTable extends DBSchemaObject {

	@YamlOrder(4)
	private String comment;

	public DBTable() {
		super("", new StringProperties(), "", "", Collections.emptySet());
	}

	public DBTable(String name, StringProperties options, String schema, String owner, Set<String> dependencies, String comment) {
		super(name, options, schema, owner, dependencies);
		this.comment = comment == null ? "" : comment;
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
		this.comment = comment == null ? "" : comment;
	}

	public String getComment() {
		return this.comment;
	}

	public static class OnlyNameDBTable extends DBTable{

		public OnlyNameDBTable(String name, String schema) {
			super(name, new StringProperties(), schema, "", Collections.emptySet(), "");
		}

		@Override
		public String getHash() {
			throw new NotImplementedExceptionDBGitRuntime();
		}

		@Override
		public void setComment(String comment) {
			throw new NotImplementedExceptionDBGitRuntime();
		}

		@Override
		public String getComment() {
			throw new NotImplementedExceptionDBGitRuntime();
		}

		@Override
		public Set<String> getDependencies() {
			throw new NotImplementedExceptionDBGitRuntime();
		}

		@Override
		public void setDependencies(Set<String> dependencies) {
			throw new NotImplementedExceptionDBGitRuntime();
		}

		@Override
		public String getOwner() {
			throw new NotImplementedExceptionDBGitRuntime();
		}

		@Override
		public void setOwner(String owner) {
			throw new NotImplementedExceptionDBGitRuntime();
		}

		@Override
		public StringProperties getOptions() {
			throw new NotImplementedExceptionDBGitRuntime();
		}

		@Override
		public void setOptions(StringProperties opt) {
			throw new NotImplementedExceptionDBGitRuntime();
		}
	}

}
