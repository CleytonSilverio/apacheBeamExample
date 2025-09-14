package com.beam.teste.apachebeam.repository;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.values.PCollection;

import com.beam.teste.apachebeam.model.Parcel;

public class ClientRepository extends BeamJdbcRepository<Parcel> {

	public ClientRepository(DataSourceConfiguration config) {
		super(config, rs -> new Parcel(rs.getInt("id"), rs.getInt("user_id"), rs.getBigDecimal("amount"),
				rs.getDate("due_date").toLocalDate()));
	}

	public PCollection<Parcel> findOpenParcels(Pipeline pipeline) {

		String query = "SELECT id, user_id, amount, due_date FROM parcel WHERE due_date >= CURRENT_DATE";

		SchemaRegistry registry = SchemaRegistry.createDefault();

		RowMapper<Parcel> rowMapper = rs -> new Parcel(rs.getInt("id"), rs.getInt("user_id"),
				rs.getBigDecimal("amount"), rs.getDate("due_date").toLocalDate());
		try {
			return pipeline.apply("Read Open Parcels",
					JdbcIO.<Parcel>read().withDataSourceConfiguration(config).withQuery(query).withRowMapper(rowMapper))
					.setCoder(registry.getSchemaCoder(Parcel.class));
		} catch (NoSuchSchemaException e) {
			throw new RuntimeException("Schema não encontrado para Parcel", e);
		}

	}

	public void saveNewParcel(PCollection<Parcel> parcels) {

		parcels.apply("Write Parcels",
				JdbcIO.<Parcel>write().withDataSourceConfiguration(config)
						.withStatement("INSERT INTO parcel (id, user_id, amount, due_date) VALUES (?, ?, ?, ?)")
						.withPreparedStatementSetter((parcel, stmt) -> {
							stmt.setInt(1, parcel.getId());
							stmt.setInt(2, parcel.getUserId());
							stmt.setBigDecimal(3, parcel.getAmount());
							stmt.setDate(4, java.sql.Date.valueOf(parcel.getDueDate()));
						}));

	}

}
