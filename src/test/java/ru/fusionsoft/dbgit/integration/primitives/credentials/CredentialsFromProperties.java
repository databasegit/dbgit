package ru.fusionsoft.dbgit.integration.primitives.credentials;

public class CredentialsFromProperties extends CredentialsEnvelope {
    public CredentialsFromProperties(String usrPropName, String pwdPropName) {
        super(()-> {
            final String usrValue = System.getProperty(usrPropName); 
            final String pwdValue = System.getProperty(pwdPropName); 
            if (usrValue == null || usrValue.isEmpty() || pwdValue == null || pwdValue.isEmpty()) {
                throw new Exception("Could not obtain credentials from props");
            }
            return new SimpleCredentials(usrValue, pwdValue);
        });
    }
}
