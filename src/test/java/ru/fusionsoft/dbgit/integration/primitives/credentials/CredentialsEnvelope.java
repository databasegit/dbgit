package ru.fusionsoft.dbgit.integration.primitives.credentials;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalar;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalarOf;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.StickyScalar;

public class CredentialsEnvelope implements Credentials {
    private final SafeScalar<Credentials> credentialsScalar;

    public CredentialsEnvelope(Scalar<Credentials> credentialsScalar) {
        this.credentialsScalar = new SafeScalarOf<>(new StickyScalar<>(credentialsScalar));
    }

    @Override
    public final String username()  {
        return this.credentialsScalar.value().username();
    }

    @Override
    public final String password()  {
        return this.credentialsScalar.value().password();
    }
}
