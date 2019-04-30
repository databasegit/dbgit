package ru.fusionsoft.dbgit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

//import junit.framework.TestCase;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.MetaObjOptions;
import ru.fusionsoft.dbgit.meta.MetaObjectFactory;
import ru.fusionsoft.dbgit.meta.MetaSchema;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.utils.StringProperties;

public class MetaObjectTest /*extends TestCase*/ {
	public final String targetPath = "target"; 
	
	/*
	public void testMetaTable() {
		DBTable tbl = new DBTable();
		
		tbl.setName("mytable");
		tbl.setSchema("myschema");
		tbl.getOptions().addChild("owner", "postgres");
		tbl.getOptions().addChild("table_space", "pg_default");
		StringProperties sub = (StringProperties)tbl.getOptions().addChild("sub");
		sub.addChild("sub1", "val1");
		sub.addChild("sub2", "val2");
		
		MetaTable tblMeta = new MetaTable(tbl);
		
		DBTableField field = new DBTableField();		
		field.setName("id");
		field.setTypeSQL("integer");
		field.setIsPrimaryKey(true);
		tblMeta.getFields().put(field.getName(), field);
		
		field = new DBTableField();
		field.setName("field1");
		field.setTypeSQL("character varying(255)");
		field.setIsPrimaryKey(false);
		tblMeta.getFields().put(field.getName(), field);
		
		field = new DBTableField();
		field.setName("field2");
		field.setTypeSQL("timestamp without time zone");
		tblMeta.getFields().put(field.getName(), field);
		
		field = new DBTableField();
		field.setName("field3");
		field.setTypeSQL("integer");
		tblMeta.getFields().put(field.getName(), field);
		
		DBIndex idx = new DBIndex();
		idx.setName("idx1");
		idx.setSchema("myschema");
		idx.setSql("CREATE INDEX idx1 ON crtd.notice  USING btree  (depart);");
		tblMeta.getIndexes().put(idx.getName(), idx);
		
		idx = new DBIndex();
		idx.setName("idx2");
		idx.setSchema("myschema");
		idx.setSql("CREATE INDEX idx2 ON crtd.notice  USING btree  (depart);");
		tblMeta.getIndexes().put(idx.getName(), idx);
		
		DBConstraint ct = new DBConstraint();
		ct.setName("fk1");
		ct.setSchema("myschema");
		ct.setSql("ALTER TABLE crtd.notice ADD CONSTRAINT fk1 FOREIGN KEY (arrive) REFERENCES crtd.ptstation (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;");
		tblMeta.getConstraints().put(ct.getName(), ct);
		
		ct = new DBConstraint();
		ct.setName("fk2");
		ct.setSchema("myschema");
		ct.setSql("ALTER TABLE crtd.notice ADD CONSTRAINT fk2 FOREIGN KEY (arrive) REFERENCES crtd.ptstation (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;");
		tblMeta.getConstraints().put(ct.getName(), ct);
		
		try {
			String filename = targetPath+"/test.yaml";
			FileOutputStream out = new FileOutputStream(filename);
			tblMeta.serialize(out);
			out.close();
			
				
			File file = new File(filename);
			FileInputStream fis = new FileInputStream(file);
			MetaTable meta2 = (MetaTable)tblMeta.deSerialize(fis);
			fis.close();
	        
	        assertEquals("Assert meta name!", "myschema/mytable.tbl", meta2.getName());
	        assertEquals("Assert meta name!", "mytable", meta2.getTable().getName());
	        assertEquals("Assert meta name!", "val1", meta2.getTable().getOptions().xPath("sub/sub1").getData());
	        
	        assertEquals("Assert meta hash!", tblMeta.getHash(), meta2.getHash());
	        //assertEquals("Assert meta hash!", tblMeta.getHash(), "hash");
	        

		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(e.getMessage(), false );
		}        
    }
    
    */
	
	public void testMetaShema() throws Exception {
		DBSchema sh = new DBSchema("myshema");
    	StringProperties pr = sh.getOptions();
    	pr.setData("info value");
    	pr.addChild("param1", "val1");
    	pr.addChild("param2", "val2");
    	pr.addChild("param3", "val3");
    	
    	StringProperties sub = pr.addChild("subparams");
    	sub.addChild("subparam1", "asd1");
    	sub.addChild("subparam2", "asd2");
    	
    	MetaObjOptions meta = (MetaObjOptions)MetaObjectFactory.createMetaObject(DBGitMetaType.DBGitSchema);     	
    	meta.setObjectOption(sh);
    	
    	//assertEquals("Assert hash!", meta.getHash(), "5c376e1836f4cbc763808fe077a84f2eaf9cdb9dc7e22107fc44a9567f4cf264");
    	
    	/*
    	System.out.println(meta.getHash());
    	
    	System.out.println(pr.toString());
    	*/
    	//TODO to yaml and assert    	
	}
}
