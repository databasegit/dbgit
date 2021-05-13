package ru.fusionsoft.dbgit.integration.primitives.path.specific;

import java.nio.file.Paths;
import ru.fusionsoft.dbgit.integration.primitives.path.PathEnvelope;

public class CurrentWorkingDirectory extends PathEnvelope {
    public CurrentWorkingDirectory(){
        super(()-> {
            return Paths.get("");
        });
    }
}
