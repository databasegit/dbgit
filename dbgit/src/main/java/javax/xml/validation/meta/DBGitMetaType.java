package javax.xml.validation.meta;

public enum DBGitMetaType {
	DBGitSequence("seq"){		
		public Class<?> getMetaClass() {
			return MetaSql.class;
		}
	},
	
	DBGitTable("tbl"){		
		public Class<?> getMetaClass() {
			return MetaTable.class;
		}
	},
	
    DbGitPakage("pkg") {		
		public Class<?> getMetaClass() {
			return MetaSql.class;
		}
	},
    
    DbGitTrigger("trg") {		
		public Class<?> getMetaClass() {
			return MetaSql.class;
		}
	},
	
	DbGitProcedure("prc") {		
		public Class<?> getMetaClass() {
			return MetaSql.class;
		}
	},
	
	DbGitFunction("fnc") {		
		public Class<?> getMetaClass() {
			return MetaSql.class;
		}
	},
	
	DbGitView("vw") {		
		public Class<?> getMetaClass() {
			return MetaSql.class;
		}
	},
	
	DbGitTableData("csv") {		
		public Class<?> getMetaClass() {
			return MetaTableData.class;
		}
	},
	
	DbGitBlob("blob") {		
		public Class<?> getMetaClass() {
			return MetaBlobData.class;
		}
	}
	;
	
	private final String val;
	
	DBGitMetaType(String val) {
		this.val = val;
	}
	
	public String getValue() {
        return val;
    }
	
	public abstract Class<?> getMetaClass();
}
