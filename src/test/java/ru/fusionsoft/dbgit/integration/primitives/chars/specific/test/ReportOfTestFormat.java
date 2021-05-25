package ru.fusionsoft.dbgit.integration.primitives.chars.specific.test;

import java.text.MessageFormat;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;

public class ReportOfTestFormat extends CharSequenceEnvelope {
    public ReportOfTestFormat(LabelOfTestRun label, CharSequence description, CharSequence subjectDetails, CharSequence testDetails) {
        super(() -> {
            final CharSequence subjectPart = String.valueOf(subjectDetails).trim().isEmpty()
                ? ""
                : "\n" + new CharsOfLines(new LinesOf(subjectDetails), "|   ");
            final CharSequence testPart = String.valueOf(testDetails).trim().isEmpty()
                ? ""
                : "\n" + new CharsOfLines(new LinesOf(testDetails), "|   ");

            return MessageFormat.format(
                "{0} \"{1}\"{2}{3}"
                , label
                , description
                , subjectPart
                , testPart
            );
        });
    }

    public ReportOfTestFormat(LabelOfTestRun label, CharSequence description, CharSequence details) {
        this(label, description, details, "");
    }

    public ReportOfTestFormat(LabelOfTestRun label, CharSequence description) {
        this(label, description, "");
    }
}
