package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalar;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalarOf;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.StickyScalar;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;

public class ArgsDbGitLink implements Args {
    private final SafeScalar<Args> argsScalar;

    public ArgsDbGitLink(Scalar<Args> origin) {
        this.argsScalar = new SafeScalarOf<Args>(new StickyScalar<>(origin));
    }
    public ArgsDbGitLink(CharSequence url, CharSequence database, CharSequence user, CharSequence pass) {
        this(()-> {
            return new ArgsExplicit(
                "link",
                url + "/" + database,
                "user=" + user,
                "password=" + pass
            );
        });
    }
    @Override
    public final CharSequence[] values() {
        return argsScalar.value().values();
    }

}
