package ru.fusionsoft.dbgit.integration.primitives.path.specific;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import ru.fusionsoft.dbgit.integration.primitives.path.PathEnvelope;

public class ProjectTestResourcesCleanDirectoryPath extends PathEnvelope {

    public ProjectTestResourcesCleanDirectoryPath() {
        this(
            String.format(
                "%#X", 
                ThreadLocalRandom.current().nextInt(
                    0,
                    32
                )
            )
        );
    }

    public ProjectTestResourcesCleanDirectoryPath(String name) {
        super(() -> {
            final File directory = new File(
                "target/itoutput/" + name
            );
            directory.mkdirs();
            FileUtils.cleanDirectory(directory);
            return directory.toPath();
        });
    }
}
