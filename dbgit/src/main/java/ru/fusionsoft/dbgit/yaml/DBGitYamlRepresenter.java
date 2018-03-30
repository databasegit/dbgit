package ru.fusionsoft.dbgit.yaml;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBGitYamlRepresenter extends Representer {
	public DBGitYamlRepresenter() {        
        this.representers.put(StringProperties.class, new RepresentYamlStringProperties());
    }
	
	private class RepresentYamlStringProperties implements Represent {
		public Node representData(Object data) {        
        	StringProperties tree = (StringProperties) data;        
        	
        	if (tree.getChildren().size() > 0) {
        		return representMapping(Tag.MAP, tree.getChildren(), DumperOptions.FlowStyle.AUTO);
        	}
        	if (tree.getData() == null) {
        		return representScalar(Tag.STR, "");
        	}
        	
            return representScalar(Tag.STR, tree.getData());
        }
    }
}
