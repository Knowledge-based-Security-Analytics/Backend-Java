package com.ur.ifs;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

//custom Annotation
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaOutput {
    String value();
}
