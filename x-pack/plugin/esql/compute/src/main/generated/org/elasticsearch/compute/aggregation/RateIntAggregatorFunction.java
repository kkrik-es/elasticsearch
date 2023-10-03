// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;

/**
 * {@link AggregatorFunction} implementation for {@link RateIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class RateIntAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("rate", ElementType.BYTES_REF)  );

  private final RateStates.SingleState state;

  private final List<Integer> channels;

  public RateIntAggregatorFunction(List<Integer> channels, RateStates.SingleState state) {
    this.channels = channels;
    this.state = state;
  }

  public static RateIntAggregatorFunction create(List<Integer> channels) {
    return new RateIntAggregatorFunction(channels, RateIntAggregator.initSingle());
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public void addRawInput(Page page) {
    Block uncastBlock = page.getBlock(channels.get(0));
    if (uncastBlock.areAllValuesNull()) {
      return;
    }
    IntBlock block = (IntBlock) uncastBlock;
    IntVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector);
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(IntVector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      RateIntAggregator.combine(state, vector.getInt(i));
    }
  }

  private void addRawBlock(IntBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        RateIntAggregator.combine(state, block.getInt(i));
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    BytesRefVector rate = page.<BytesRefBlock>getBlock(channels.get(0)).asVector();
    assert rate.getPositionCount() == 1;
    BytesRef scratch = new BytesRef();
    RateIntAggregator.combineIntermediate(state, rate.getBytesRef(0, scratch));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset) {
    state.toIntermediate(blocks, offset);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset) {
    blocks[offset] = RateIntAggregator.evaluateFinal(state);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("channels=").append(channels);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    state.close();
  }
}
