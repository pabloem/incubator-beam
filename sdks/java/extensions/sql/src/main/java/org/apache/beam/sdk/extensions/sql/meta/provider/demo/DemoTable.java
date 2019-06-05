/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.meta.provider.demo;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;

class DemoTable extends BaseBeamTable implements Serializable {
  static public final Schema tableSchema = Schema.of(
      Field.of("name", FieldType.STRING),
      Field.of("team", FieldType.STRING),
      Field.of("score", FieldType.INT32),
      Field.of("event_time", FieldType.DATETIME));

  static private final List<String> players = ImmutableList.of(
      "Mr Lopez",
      "El Santo",
      "La Parca",
      "Mil Mascaras",
      "Blue Demon",
      "Dr Wagner Jr",
      "Canek",
      "Perro Aguayo",
      "Atlantis",
      "Blue Panther",
      "Volador Jr",
      "Tinieblas",
      "Brazo de Plata");

  Integer elementsPerSecond = 5;


  DemoTable(Table table) {
    super(tableSchema);
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply(GenerateSequence.from(0).withRate(elementsPerSecond, Duration.standardSeconds(1)))
        .apply(ParDo.of(
                new DoFn<Long, Row>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    Random rnd = new Random(context.element());
                    Integer playerIndex = rnd.nextInt(players.size());

                    String name = players.get(playerIndex);

                    String team = "red";
                    if(playerIndex % 2 == 1) {
                      team = "blue";
                    }

                    Integer score = rnd.nextInt(100);
                    Row result = Row.withSchema(tableSchema)
                        .addValues(name, team, score, Instant.now()).build();
                    context.output(result);
                  }
                }))
        .setRowSchema(getSchema());
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException("buildIOWriter unsupported!");
  }
}
