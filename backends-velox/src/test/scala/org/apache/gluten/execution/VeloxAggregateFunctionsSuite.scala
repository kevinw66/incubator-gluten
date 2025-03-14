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
package org.apache.gluten.execution

import org.apache.gluten.config.VeloxConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

abstract class VeloxAggregateFunctionsSuite extends VeloxWholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.gluten.sql.mergeTwoPhasesAggregate.enabled", "true")
      .set("spark.gluten.sql.columnar.backend.velox.glogVerboseLevel", "0")
      .set("spark.gluten.sql.columnar.backend.velox.glogSeverityLevel", "0")
  }

  test("my debug test") {
    runQueryAndCompare(s"""
                          |select sum(l_linenumber), l_orderkey from lineitem
                          |group by l_orderkey having l_orderkey > 59800;
                          |""".stripMargin) {
      df =>
        {
          logWarning(s"df: ${df.queryExecution}")
        }
    }
  }
}

class VeloxAggregateFunctionsDefaultSuite extends VeloxAggregateFunctionsSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // Disable flush. This may cause spilling to happen on partial aggregations.
      .set(VeloxConfig.VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED.key, "false")
  }
}

class VeloxAggregateFunctionsFlushSuite extends VeloxAggregateFunctionsSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // To test flush behaviors, set low flush threshold to ensure flush happens.
      .set(VeloxConfig.VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED.key, "true")
      .set(VeloxConfig.ABANDON_PARTIAL_AGGREGATION_MIN_PCT.key, "1")
      .set(VeloxConfig.ABANDON_PARTIAL_AGGREGATION_MIN_ROWS.key, "10")
  }

  test("flushable aggregate rule") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1k") {
      runQueryAndCompare("select distinct l_partkey from lineitem") {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[RegularHashAggregateExecTransformer]))
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[FlushableHashAggregateExecTransformer]))
      }
    }
  }

  test("flushable aggregate rule - agg input already distributed by keys") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1k") {
      runQueryAndCompare(
        "select * from (select distinct l_orderkey,l_partkey from lineitem) a" +
          " inner join (select l_orderkey from lineitem limit 10) b" +
          " on a.l_orderkey = b.l_orderkey limit 10") {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[RegularHashAggregateExecTransformer]))
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[FlushableHashAggregateExecTransformer]))
      }
    }
  }

  test("flushable aggregate decimal sum") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1k") {
      runQueryAndCompare("select sum(l_quantity) from lineitem") {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[RegularHashAggregateExecTransformer]))
          assert(
            executedPlan.exists(plan => plan.isInstanceOf[FlushableHashAggregateExecTransformer]))
      }
    }
  }
}

object VeloxAggregateFunctionsSuite {
  val GROUP_SETS_TEST_SQL: String =
    "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
      "where l_orderkey < 3 group by ROLLUP(l_orderkey, l_partkey) " +
      "order by l_orderkey, l_partkey "
}
