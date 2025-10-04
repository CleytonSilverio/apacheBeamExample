package com.beam.teste.apachebeam.mapper;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.LocalDate;

import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

import com.beam.teste.apachebeam.model.Parcel;

@Mapper
public interface RowParcelMapper {
    public static final RowParcelMapper INSTANCE = Mappers.getMapper(RowParcelMapper.class);

    @Mapping(target = "id", expression = "java(getInt(row, \"id\"))")
    @Mapping(target = "userId", expression = "java(getInt(row, \"user_id\"))")
    @Mapping(target = "amount", expression = "java(getBigDecimal(row, \"amount\"))")
    @Mapping(target = "dueDate", expression = "java(getLocalDate(row, \"due_date\"))")
    public abstract Parcel rowToParcel(Row row);
    
    default Parcel logRowToParcel(Row row) {
        Parcel parcel = rowToParcel(row);
        System.out.println("[RowParcelMapper] Row: " + row);
        System.out.println("[RowParcelMapper] Parcel criado: " + parcel + " | hashCode: " + (parcel != null ? parcel.hashCode() : "null"));
        return parcel;
    }

    default int getInt(Row row, String field) {
        Object value = row.getBaseValue(field, Object.class);
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Number) return ((Number) value).intValue();
        return 0;
    }

    default BigDecimal getBigDecimal(Row row, String field) {
        Object value = row.getBaseValue(field, Object.class);
        if (value instanceof BigDecimal) return (BigDecimal) value;
        if (value instanceof Number) return BigDecimal.valueOf(((Number) value).doubleValue());
        return null;
    }

    default LocalDate getLocalDate(Row row, String field) {
        Object value = row.getBaseValue(field, Object.class);
        if (value instanceof LocalDate) return (LocalDate) value;
        if (value instanceof java.sql.Date) return ((java.sql.Date) value).toLocalDate();
        if (value instanceof Instant) {
            Instant instant = (Instant) value;
            return instant.toDateTime().toLocalDate().toDate().toInstant()
                .atZone(java.time.ZoneId.systemDefault()).toLocalDate();
        }
        return null;
    }
}