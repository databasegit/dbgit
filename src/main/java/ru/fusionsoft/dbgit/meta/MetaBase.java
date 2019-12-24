package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.DumperOptions.ScalarStyle;
import org.yaml.snakeyaml.nodes.Tag;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.yaml.DBGitYamlConstructor;
import ru.fusionsoft.dbgit.yaml.DBGitYamlRepresenter;
import ru.fusionsoft.dbgit.yaml.YamlOrder;


/**
 * Base class for all meta objects
 * @author mikle
 *
 */
public abstract class MetaBase implements IMetaObject {
	@YamlOrder(0)
	protected String name;
	
	@YamlOrder(1)
	protected DbType dbType;
	
	@YamlOrder(1)
	protected String dbVersion;
	
	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public void setDbType(DbType dbType) {
		this.dbType = dbType;		
	}
	
	@Override
	public DbType getDbType() {
		return dbType;
	}
	
	@Override
	public void setDbVersion(String dbVersion) {
		this.dbVersion = dbVersion;		
	}
	
	@Override
	public String getDbVersion() {
		return dbVersion;
	}
	
	@Override
	public void setName(String name) throws ExceptionDBGit {
		this.name = name;		
	}

	@Override
	public String getFileName() {
		return getName();
	}
	
	/**
	 * <div class="en">When you save the yaml object, the library ignores properties for which there is no getter and setter</div>
	 * <div class="ru">При сохранении объекта yaml библиотека игнорирует свойства для которых нет геттера и сеттера</div>
	 * @param stream
	 * @throws IOException
	 */
	public boolean yamlSerialize(OutputStream stream) throws IOException {
        Yaml yaml = createYaml();     
        String output = yaml.dumpAs(this, Tag.MAP, DumperOptions.FlowStyle.BLOCK);
        
        stream.write(output.getBytes(Charset.forName("UTF-8")));
        return true;
	}
	
	public IMetaObject yamlDeSerialize(InputStream stream) {
        Yaml yaml = createYaml();
        
        IMetaObject meta = yaml.loadAs(stream, this.getClass());        
        return meta;
	}
	
	public Yaml createYaml() {
		DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);               
        options.setPrettyFlow(true);
        Yaml yaml = new Yaml(new DBGitYamlConstructor(), new DBGitYamlRepresenter(), options);
        return yaml;
	}
	
	@Override
	public int addToGit() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		dbGit.addFileToIndexGit(DBGitPath.DB_GIT_PATH+"/"+getFileName());
		return 1;
	}
	
	@Override
	public int removeFromGit() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		dbGit.removeFileFromIndexGit(DBGitPath.DB_GIT_PATH+"/"+getFileName());
		return 1;
	}
	
	public void setDbType() {
		try {
			setDbType(AdapterFactory.createAdapter().getDbType());
		} catch (ExceptionDBGit e) {
			throw new ExceptionDBGitRunTime(e.getLocalizedMessage());
		}

	}
	
	public void setDbVersion() {
		try {
			setDbVersion(AdapterFactory.createAdapter().getDbVersion());
		} catch (ExceptionDBGit e) {
			throw new ExceptionDBGitRunTime(e.getLocalizedMessage());
		}

	}

	
}
