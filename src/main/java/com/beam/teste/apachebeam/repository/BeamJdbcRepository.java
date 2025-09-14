package com.beam.teste.apachebeam.repository;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.beam.teste.apachebeam.config.PreparedStatementSetter;
import com.beam.teste.apachebeam.config.RowMapper;

public class BeamJdbcRepository<T> {

    protected final DataSourceConfiguration config;
    private final RowMapper<T> rowMapper;

    public BeamJdbcRepository(DataSourceConfiguration config, RowMapper<T> rowMapper) {
        this.config = config;
        this.rowMapper = rowMapper;
    }

    public PCollection<T> read(Pipeline pipeline, String query) {
        return pipeline.apply(
            JdbcIO.<T>read()
                .withDataSourceConfiguration(config)
                .withQuery(query)
                .withRowMapper(rowMapper::mapRow)
                .withOutputParallelization(false)
        );
    }

    public PDone write(PCollection<T> input, String insertStatement, PreparedStatementSetter<T> setter) {
        return input.apply(
            JdbcIO.<T>write()
                .withDataSourceConfiguration(config)
                .withStatement(insertStatement)
                .withPreparedStatementSetter(setter::setValues)
        );
    }
}

