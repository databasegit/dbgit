package ru.fusionsoft.dbgit.integration.primitives.args;

import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalar;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalarOf;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.StickyScalar;

public class ArgsLink implements Args {

    private final SafeScalar<Args> argsScalar;

    public ArgsLink(Scalar<Args> orign) {
        this.argsScalar = new SafeScalarOf<Args>(new StickyScalar<>(orign));
    }

    @Override
    public final CharSequence[] values() {
        return argsScalar.value().values();
    }
    public ArgsLink(String url, String database, String user, String pass) {
        this(()-> {
            return new ArgsExplicit(
                "link",
                url + "/" + database,
                "user=" + user,
                "password=" + pass
            );
        });
    }

}
