package org.influxdb.impl;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBMapperException;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

public class InfluxDBMapper extends InfluxDBResultMapper {

  private final InfluxDB influxDB;

  public InfluxDBMapper(final InfluxDB influxDB) {
    this.influxDB = influxDB;
  }

  public <T> List<T> query(final Query query, final Class<T> clazz) {
    throwExceptionIfMissingAnnotation(clazz);
    QueryResult queryResult = influxDB.query(query);
    return toPOJO(queryResult, clazz);
  }

  public <T> List<T> query(final Class<T> clazz) {
    throwExceptionIfMissingAnnotation(clazz);

    String measurement = getMeasurementName(clazz);
    String database = getDatabaseName(clazz);

    if ("[unassigned]".equals(database)) {
      throw new IllegalArgumentException(
          Measurement.class.getSimpleName()
              + " of class "
              + clazz.getName()
              + " should specify a database value for this operation");
    }

    QueryResult queryResult = influxDB.query(new Query("SELECT * FROM " + measurement, database));
    return toPOJO(queryResult, clazz);
  }

  public <T> void save(final T model) {
    Class<?> modelType = model.getClass();
    throwExceptionIfMissingAnnotation(modelType);
    cacheMeasurementClass(modelType);

    try {
      Point point = createPoint(model, true, true);

      String database = getDatabaseName(modelType);
      String retentionPolicy = getRetentionPolicy(modelType);

      if ("[unassigned]".equals(database)) {
        influxDB.write(point);
      } else {
        influxDB.write(database, retentionPolicy, point);
      }

    } catch (IllegalAccessException e) {
      throw new InfluxDBMapperException(e);
    }
  }

  public <T> void deleteMeasurementsByTagsWithoutTime(final T model) {
    delete(model, null);
  }

  public <T> void deleteMeasurementsByTagsSinceTime(final T model) {
    delete(model, ">");
  }

  public <T> void deleteMeasurementsByTagsUntilTime(final T model) {
    delete(model, "<");
  }

  private <T> void delete(final T model, String timeRelationalOperator) {
    Class<?> modelType = model.getClass();
    throwExceptionIfMissingAnnotation(modelType);
    cacheMeasurementClass(modelType);

    try {
      String database = getDatabaseName(modelType);
      Point point = createPoint(model, false, false);
      Query query = new Query(point.deleteQuery(timeRelationalOperator), database);

      influxDB.query(query);

    } catch (IllegalAccessException e) {
      throw new InfluxDBMapperException(e);
    }
  }

  public <T> Point createPoint(final T model, boolean setDefaultTime, boolean includeFields) throws IllegalAccessException {
    Class<?> modelType = model.getClass();
    ConcurrentMap<String, Field> colNameAndFieldMap = getColNameAndFieldMap(modelType);
    String measurement = getMeasurementName(modelType);
    TimeUnit timeUnit = getTimeUnit(modelType);
    Point.Builder pointBuilder = Point.measurement(measurement);

    if(setDefaultTime){
      long time = timeUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      pointBuilder.time(time, timeUnit);
    }

    for (String key : colNameAndFieldMap.keySet()) {
      Field field = colNameAndFieldMap.get(key);
      Column column = field.getAnnotation(Column.class);
      String columnName = column.name();
      Class<?> fieldType = field.getType();

      if (!field.isAccessible()) {
        field.setAccessible(true);
      }

      Object value = field.get(model);

      if (column.nullable() && value == null) {
        continue;
      }

      if (column.tag()) {
        /** Tags are strings either way. */
        pointBuilder.tag(columnName, value.toString());
      } else if ("time".equals(columnName)) {
        if (value != null) {
          setTime(pointBuilder, fieldType, timeUnit, value);
        }
      } else if (includeFields) {
        setField(pointBuilder, fieldType, columnName, value);
      }
    }

    Point point = pointBuilder.build(includeFields);

    return point;
  }

  private void setTime(
      final Point.Builder pointBuilder,
      final Class<?> fieldType,
      final TimeUnit timeUnit,
      final Object value) {
    if (Instant.class.isAssignableFrom(fieldType)) {
      Instant instant = (Instant) value;
      long time = timeUnit.convert(instant.toEpochMilli(), TimeUnit.MILLISECONDS);
      pointBuilder.time(time, timeUnit);
    } else {
      throw new InfluxDBMapperException(
          "Unsupported type " + fieldType + " for time: should be of Instant type");
    }
  }

  private void setField(
      final Point.Builder pointBuilder,
      final Class<?> fieldType,
      final String columnName,
      final Object value) {
    if (boolean.class.isAssignableFrom(fieldType) || Boolean.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (boolean) value);
    } else if (long.class.isAssignableFrom(fieldType) || Long.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (long) value);
    } else if (double.class.isAssignableFrom(fieldType)
        || Double.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (double) value);
    } else if (int.class.isAssignableFrom(fieldType) || Integer.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (int) value);
    } else if (String.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (String) value);
    } else {
      throw new InfluxDBMapperException(
          "Unsupported type " + fieldType + " for column " + columnName);
    }
  }

}
