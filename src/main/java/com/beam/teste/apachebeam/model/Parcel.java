package com.beam.teste.apachebeam.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@DefaultSchema(JavaFieldSchema.class)
public class Parcel implements Serializable{

	private static final long serialVersionUID = 1L;
	private int id;
	private int userId;
	private BigDecimal amount;
	private LocalDate dueDate;
}