/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.streaming

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

/**
 * Simple test exercising join like semantics built using transformWithState.
 */
class TransformWithStateJoinSuite extends StreamTest with StateStoreMetricsTest {

  import testImplicits._

  case class ClickEvent(userId: String, ts: Timestamp)
  case class InfoEvent(userId: String, ts: Timestamp)
  case class EventWrapper(userId: String, ts: Timestamp, kind: String)
  case class EnrichedUserEvent(userId: String, clickTs: Timestamp, infoTs: Timestamp)

  class CustomStreamJoinProcessor
    extends StatefulProcessor[String, EventWrapper, EnrichedUserEvent] {

    @transient private var clickState: ValueState[Timestamp] = _
    @transient private var infoState: ValueState[Timestamp] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      clickState = getHandle.getValueState[Timestamp]("click", Encoders.TIMESTAMP, TTLConfig.NONE)
      infoState = getHandle.getValueState[Timestamp]("info", Encoders.TIMESTAMP, TTLConfig.NONE)
    }

    override def handleInputRows(
        key: String,
        rows: Iterator[EventWrapper],
        timerValues: TimerValues): Iterator[EnrichedUserEvent] = {
      var newData = false
      rows.foreach { r =>
        if (r.kind == "click") {
          clickState.update(r.ts)
          newData = true
        } else {
          infoState.update(r.ts)
          newData = true
        }
      }
      if (newData && clickState.exists() && infoState.exists()) {
        getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 1000)
      }
      Iterator.empty
    }

    override def handleExpiredTimer(
        key: String,
        timerValues: TimerValues,
        expiredTimerInfo: ExpiredTimerInfo): Iterator[EnrichedUserEvent] = {
      if (clickState.exists() && infoState.exists()) {
        val out = EnrichedUserEvent(key, clickState.get(), infoState.get())
        clickState.clear()
        infoState.clear()
        Iterator(out)
      } else Iterator.empty
    }
  }

  private def createStream[T](ms: MemoryStream[T]): Dataset[T] = {
    ms.toDS()
  }

  test("join events across streams with timer delay") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName) {
      val clickStream = MemoryStream[ClickEvent]
      val infoStream = MemoryStream[InfoEvent]
      val unionDS = clickStream.toDS().map(e => EventWrapper(e.userId, e.ts, "click"))
        .union(infoStream.toDS().map(e => EventWrapper(e.userId, e.ts, "info")))
        .withWatermark("ts", "5 seconds")
        .groupByKey(_.userId)
        .transformWithState(
          new CustomStreamJoinProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(unionDS) (
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(clickStream, ClickEvent("u1", new Timestamp(0))),
        AddData(infoStream, InfoEvent("u1", new Timestamp(0))),
        AdvanceManualClock(1000),
        CheckNewAnswer(
          EnrichedUserEvent("u1", new Timestamp(0), new Timestamp(0))),
        StopStream
      )
    }
  }

  test("late events still join after watermark") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName) {
      val clickStream = MemoryStream[ClickEvent]
      val infoStream = MemoryStream[InfoEvent]
      val unionDS = clickStream.toDS().map(e => EventWrapper(e.userId, e.ts, "click"))
        .union(infoStream.toDS().map(e => EventWrapper(e.userId, e.ts, "info")))
        .withWatermark("ts", "2 seconds")
        .groupByKey(_.userId)
        .transformWithState(
          new CustomStreamJoinProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(unionDS) (
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(clickStream, ClickEvent("u1", new Timestamp(1000))),
        AddData(infoStream, InfoEvent("u1", new Timestamp(1000))),
        AdvanceManualClock(1000),
        CheckNewAnswer(
          EnrichedUserEvent("u1", new Timestamp(1000), new Timestamp(1000))),
        AddData(clickStream, ClickEvent("u2", new Timestamp(4000))),
        AdvanceManualClock(1000),
        AddData(infoStream, InfoEvent("u2", new Timestamp(2000))), // late relative to watermark
        AdvanceManualClock(1000),
        CheckNewAnswer(
          EnrichedUserEvent("u2", new Timestamp(4000), new Timestamp(2000))),
        StopStream
      )
    }
  }
}

