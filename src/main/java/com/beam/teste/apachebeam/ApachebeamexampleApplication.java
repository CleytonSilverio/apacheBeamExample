package com.beam.teste.apachebeam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

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
		
		openParcels.apply("log", MapElements.via(new SimpleFunction<Parcel, Parcel>() {
			@Override
			public Parcel apply(Parcel log) {
				System.out.println("[Main] Parcel: " + log.toString() + " | hashCode: " + (log != null ? log.hashCode() : "null"));
				return log;
			}
		}));
		
		// 3) Aplica cálculo 1
		PCollection<Parcel> recalculated1 = applyCalculo(openParcels);

		// 4) Salva no banco
		repo.saveNewParcel(recalculated1);
		

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