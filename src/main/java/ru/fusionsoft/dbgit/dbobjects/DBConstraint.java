package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;
import ru.fusionsoft.dbgit.yaml.YamlOrder;

import java.util.Objects;
import java.util.Set;

public class DBConstraint extends DBSQLObject {

	@YamlOrder(4)
	private String constraintType;

	public DBConstraint(String name, StringProperties options, String schema, String owner, Set<String> dependencies, String sql, String constraintType) {
		super(name, options, schema, owner, dependencies, sql);
		this.constraintType = constraintType;
	}


	public String getConstraintType() {
		return constraintType;
	}
	public void setConstraintType(String constraintType) {
		this.constraintType = constraintType;
	}

}
