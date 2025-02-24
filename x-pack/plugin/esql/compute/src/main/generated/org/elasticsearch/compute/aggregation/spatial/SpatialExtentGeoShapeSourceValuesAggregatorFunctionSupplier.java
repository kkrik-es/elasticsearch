// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation.spatial;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialExtentGeoShapeSourceValuesAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SpatialExtentGeoShapeSourceValuesAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public SpatialExtentGeoShapeSourceValuesAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SpatialExtentGeoShapeSourceValuesAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SpatialExtentGeoShapeSourceValuesGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SpatialExtentGeoShapeSourceValuesAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SpatialExtentGeoShapeSourceValuesAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SpatialExtentGeoShapeSourceValuesGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return SpatialExtentGeoShapeSourceValuesGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "spatial_extent_geo_shape_source of valuess";
  }
}
