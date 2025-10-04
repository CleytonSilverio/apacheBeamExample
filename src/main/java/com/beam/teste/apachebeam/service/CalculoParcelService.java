package com.beam.teste.apachebeam.service;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.beam.sdk.transforms.DoFn;

import com.beam.teste.apachebeam.model.Parcel;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class CalculoParcelService extends DoFn<Parcel, Parcel> implements Serializable{

	@ProcessElement
	public void process(@Element Parcel parcel, OutputReceiver<Parcel> out) {
		System.out.println("[Service] Recebido: " + parcel + " | hashCode: " + (parcel != null ? parcel.hashCode() : "null"));
		BigDecimal update = parcel.getAmount().multiply(BigDecimal.valueOf(1.1));

		Parcel newParcel = new Parcel((parcel.getId() + 1), parcel.getUserId(), update,
				parcel.getDueDate().plusDays(1));

		log.info("Parcela atualizada: " + newParcel.toString());
		out.output(newParcel);
	}

}