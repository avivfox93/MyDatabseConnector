package com.aei.dbconnector;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(FIELD)
public @interface DBField {
	public boolean id() default false;
	public boolean readOnly() default false;
	public boolean autoIncreasment() default false;
}
