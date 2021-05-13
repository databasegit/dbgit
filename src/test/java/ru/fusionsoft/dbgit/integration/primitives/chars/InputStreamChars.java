package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class InputStreamChars extends CharSequenceEnvelope{
    public InputStreamChars(InputStream origin, String codepageName) {
        super(() -> {

            try (
                final BufferedReader reader = new BufferedReader(
                    codepageName.equals("default") 
                        ? new InputStreamReader(origin)
                        : new InputStreamReader(origin, codepageName)
                )
            ) {
                StringBuilder builder = new StringBuilder();
                String line = null;
                while (( line = reader.readLine() ) != null) {
                    builder.append("> ");
                    builder.append(line);
                    builder.append(System.getProperty("line.separator"));
                }
                return builder.toString();
            }
            
//            try (final Scanner s = new Scanner(origin)) {
//                final StringBuilder text = new StringBuilder();
//                while (s.hasNextLine()) text
//                .append("\n> ")
//                .append(s.nextLine());
//                return text.toString();
//            }
        });
    }
    
    public InputStreamChars(InputStream origin) {
        this(origin, "default");
    }
}
