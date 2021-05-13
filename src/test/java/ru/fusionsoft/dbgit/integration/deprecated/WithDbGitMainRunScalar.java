package ru.fusionsoft.dbgit.integration.deprecated;

import java.util.Arrays;
import ru.fusionsoft.dbgit.App;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;

public class WithDbGitMainRunScalar<Y> implements Scalar<Y> {
    private final Args args;
    private final Y origin;

    public WithDbGitMainRunScalar(Args args, Y origin) {
        this.args = args;
        this.origin = origin;
    }

    @Override
    public final Y value() throws Exception {
        App.main(
            Arrays.stream(this.args.values())
            .map(String::valueOf)
            .toArray(String[]::new)
        );
        return origin;
    }
}
