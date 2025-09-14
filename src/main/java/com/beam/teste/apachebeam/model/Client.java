package com.beam.teste.apachebeam.model;

import java.io.Serializable;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
@DefaultSchema(JavaFieldSchema.class)
public class Client implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private int id;
	private String name;

}
