package ru.fusionsoft.dbgit.integration.deprecated;

import java.nio.file.Paths;
import ru.fusionsoft.dbgit.integration.primitives.path.PathEnvelope;

public class SystemUserDirPath extends PathEnvelope {
    public SystemUserDirPath(){
        super(()->Paths.get(System.getProperty("user.dir")));
    }
}
