package ru.fusionsoft.dbgit.integration.primitives.chars;

public class TestSuccessMarkChars extends CharSequenceEnvelope {

    public TestSuccessMarkChars(boolean booleanValue) {
        super(()-> booleanValue ? "[TEST OK]" : "[TEST FAIL]");
    }
    
    public TestSuccessMarkChars(Exception testException) {
        super(()-> "[TEST RUNNING EXCEPTION]");
    }  
    
    public TestSuccessMarkChars(Error subjectConstructionError  ) {
        super(() -> "[TEST SUBJECT ERROR]");
    }    
    
    public TestSuccessMarkChars(RuntimeException subjectConstructionRuntimeException ) {
        super(() -> "[TEST SUBJECT ERROR]");
    }

}
