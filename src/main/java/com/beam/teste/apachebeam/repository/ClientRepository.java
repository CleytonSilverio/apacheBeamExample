package com.beam.teste.apachebeam.repository;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.beam.teste.apachebeam.mapper.RowParcelMapper;
import com.beam.teste.apachebeam.model.Parcel;

public class ClientRepository extends BeamJdbcRepository<Parcel> {

	public ClientRepository(DataSourceConfiguration config) {
		super(config, rs -> new Parcel(rs.getInt("id"), rs.getInt("user_id"), rs.getBigDecimal("amount"),
				rs.getDate("due_date").toLocalDate()));
	}

	public PCollection<Parcel> findOpenParcels(Pipeline pipeline) {
		String query = "SELECT * FROM parcel";

		return pipeline
				.apply("Read Open Parcels", JdbcIO.readRows().withDataSourceConfiguration(config).withQuery(query))
				.apply(MapElements.into(TypeDescriptor.of(Parcel.class))
						.via(row -> RowParcelMapper.INSTANCE.logRowToParcel(row)));
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