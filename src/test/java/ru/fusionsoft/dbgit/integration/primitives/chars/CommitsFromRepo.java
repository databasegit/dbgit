package ru.fusionsoft.dbgit.integration.primitives.chars;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.transport.RefSpec;

public class CommitsFromRepo {
    private final String repoUrl;
    private final String branchName;

    public CommitsFromRepo(String repoUrl, String branchName) {
        this.repoUrl = repoUrl;
        this.branchName = branchName;
    }

    public final List<String> names() throws Exception {
        final String treeName = "refs/heads/" + branchName; // tag or branch
        try (
            final Git git = new Git(
                new InMemoryRepository(
                    new DfsRepositoryDescription()
                )
            )
        ) {
            git.fetch()
            .setRemote(repoUrl)
            .setRefSpecs(
                new RefSpec(
                    "+refs/heads/" + branchName
                        + ":refs/heads/" + branchName
                )
            )
            .call();

            return Lists.newArrayList(
                git
                .log()
                .add(git.getRepository().resolve(treeName))
                .call()
            )
            .stream()
            .map(AnyObjectId::getName)
            .collect(Collectors.toList());
        }
    }
}
