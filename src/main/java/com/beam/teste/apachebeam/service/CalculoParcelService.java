package com.beam.teste.apachebeam.service;

import java.math.BigDecimal;

import org.apache.beam.sdk.transforms.DoFn;

import com.beam.teste.apachebeam.model.Parcel;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class CalculoParcelService extends DoFn<Parcel, Parcel> {

	@ProcessElement
	public void process(@Element Parcel parcel, OutputReceiver<Parcel> out) {
		BigDecimal update = parcel.getAmount().multiply(BigDecimal.valueOf(1.1));

		Parcel newParcel = new Parcel((parcel.getId() + 1), parcel.getUserId(), update,
				parcel.getDueDate().plusDays(1));

		log.info("Parcela atualizada: " + newParcel.toString());
		out.output(newParcel);
	}

}
