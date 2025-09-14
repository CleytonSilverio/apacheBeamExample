package com.beam.teste.apachebeam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.beam.teste.apachebeam.model.Parcel;
import com.beam.teste.apachebeam.repository.ClientRepository;
import com.beam.teste.apachebeam.service.CalculoParcelService;

public class ApachebeamexampleApplication {

	public static void main(String[] args) {
		// Configuração MySQL
		JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration
				.create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC")
				.withUsername("cleyton").withPassword("senha");

		Pipeline pipeline = Pipeline.create();

		ClientRepository repo = new ClientRepository(config);

		// 1) Lê parcelas
		PCollection<Parcel> openParcels = repo.findOpenParcels(pipeline);

		// 2) Debug
		openParcels.apply("Debug Parcels", MapElements.into(TypeDescriptors.strings()).via(p -> {
			System.out.println(p);
			return p.toString();
		}));

		// 3) Aplica cálculo 1
		PCollection<Parcel> recalculated1 = applyCalculo(openParcels);

		// 4) Salva no banco
		String insertSql = "INSERT INTO parcel (id, user_id, amount, due_date) VALUES (?, ?, ?, ?)";
//		repo.saveAll(recalculated1, insertSql, parcelSetter);
		
		recalculated1.apply("Save New Parcels",
			    JdbcIO.<Parcel>write()
			        .withDataSourceConfiguration(config)
			        .withStatement("INSERT INTO parcel (id, user_id, amount, due_date) VALUES (?, ?, ?, ?)")
			        .withPreparedStatementSetter((parcel, stmt) -> {
			            stmt.setInt(1, parcel.getId());
			            stmt.setInt(2, parcel.getUserId());
			            stmt.setBigDecimal(3, parcel.getAmount());
			            stmt.setDate(4, java.sql.Date.valueOf(parcel.getDueDate()));
			        })
			);

		// 5) Você pode aplicar outros cálculos diferentes
//		PCollection<Parcel> recalculated2 = applyOutroCalculo(openParcels);
//		repo.saveAll(recalculated2, insertSql, parcelSetter);

		pipeline.run().waitUntilFinish();
	}

	private static PCollection<Parcel> applyCalculo(PCollection<Parcel> parcels) {
		return parcels.apply("Recalculate Parcels", ParDo.of(new CalculoParcelService()));
	}

//	private static PCollection<Parcel> applyOutroCalculo(PCollection<Parcel> parcels) {
//		return parcels.apply("Outro Calculo", ParDo.of(new OutroCalculoService()));
//	}

}
