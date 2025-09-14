package com.beam.teste.apachebeam.config;

import java.sql.PreparedStatement;

@FunctionalInterface
public interface PreparedStatementSetter<T> {
    void setValues(T item, PreparedStatement ps) throws Exception;
}
