package ru.fusionsoft.dbgit.yaml;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.*;

@Target( ElementType.FIELD )
@Retention(value= RetentionPolicy.RUNTIME)
@Inherited
public @interface YamlOrder {
	public int value();
}
