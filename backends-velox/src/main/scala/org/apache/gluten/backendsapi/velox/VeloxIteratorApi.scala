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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.backendsapi.{BackendsApiManager, IteratorApi}
import org.apache.gluten.backendsapi.velox.VeloxIteratorApi.unescapePathName
import org.apache.gluten.config.GlutenNumaBindingInfo
import org.apache.gluten.execution._
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.metrics.{IMetrics, IteratorMetricsJniWrapper}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.substrait.rel.{LocalFilesBuilder, LocalFilesNode, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.vectorized._

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.util.{DateFormatter, TimestampFormatter}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{ExecutorManager, SparkDirectoryUtil}

import java.lang.{Long => JLong}
import java.nio.charset.StandardCharsets
import java.time.ZoneOffset
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap, UUID}

import scala.collection.JavaConverters._

class VeloxIteratorApi extends IteratorApi with Logging {

  override def genSplitInfo(
      partition: InputPartition,
      partitionSchema: StructType,
      fileFormat: ReadFileFormat,
      metadataColumnNames: Seq[String],
      properties: Map[String, String]): SplitInfo = {
    partition match {
      case f: FilePartition =>
        val (
          paths,
          starts,
          lengths,
          fileSizes,
          modificationTimes,
          partitionColumns,
          metadataColumns,
          otherMetadataColumns) =
          constructSplitInfo(partitionSchema, f.files, metadataColumnNames)
        val preferredLocations =
          SoftAffinity.getFilePartitionLocations(f)
        LocalFilesBuilder.makeLocalFiles(
          f.index,
          paths,
          starts,
          lengths,
          fileSizes,
          modificationTimes,
          partitionColumns,
          metadataColumns,
          fileFormat,
          preferredLocations.toList.asJava,
          mapAsJavaMap(properties),
          otherMetadataColumns
        )
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported input partition.")
    }
  }

  override def genSplitInfoForPartitions(
      partitionIndex: Int,
      partitions: Seq[InputPartition],
      partitionSchema: StructType,
      fileFormat: ReadFileFormat,
      metadataColumnNames: Seq[String],
      properties: Map[String, String]): SplitInfo = {
    val partitionFiles = partitions.flatMap {
      p =>
        if (!p.isInstanceOf[FilePartition]) {
          throw new UnsupportedOperationException(
            s"Unsupported input partition ${p.getClass.getName}.")
        }
        p.asInstanceOf[FilePartition].files
    }.toArray
    val locations =
      partitions.flatMap(p => SoftAffinity.getFilePartitionLocations(p.asInstanceOf[FilePartition]))
    val (
      paths,
      starts,
      lengths,
      fileSizes,
      modificationTimes,
      partitionColumns,
      metadataColumns,
      otherMetadataColumns) =
      constructSplitInfo(partitionSchema, partitionFiles, metadataColumnNames)
    LocalFilesBuilder.makeLocalFiles(
      partitionIndex,
      paths,
      starts,
      lengths,
      fileSizes,
      modificationTimes,
      partitionColumns,
      metadataColumns,
      fileFormat,
      locations.toList.asJava,
      mapAsJavaMap(properties),
      otherMetadataColumns
    )
  }

  /** Generate native row partition. */
  override def genPartitions(
      wsCtx: WholeStageTransformContext,
      splitInfos: Seq[Seq[SplitInfo]],
      leaves: Seq[LeafTransformSupport]): Seq[BaseGlutenPartition] = {
    // Only serialize plan once, save lots time when plan is complex.
    val planByteArray = wsCtx.root.toProtobuf.toByteArray

    splitInfos.zipWithIndex.map {
      case (splitInfos, index) =>
        GlutenPartition(
          index,
          planByteArray,
          splitInfos.map(_.asInstanceOf[LocalFilesNode].toProtobuf.toByteArray).toArray,
          splitInfos.flatMap(_.preferredLocations().asScala).toArray
        )
    }
  }

  private def constructSplitInfo(
      schema: StructType,
      files: Array[PartitionedFile],
      metadataColumnNames: Seq[String]) = {
    val paths = new JArrayList[String]()
    val starts = new JArrayList[JLong]
    val lengths = new JArrayList[JLong]()
    val fileSizes = new JArrayList[JLong]()
    val modificationTimes = new JArrayList[JLong]()
    val partitionColumns = new JArrayList[JMap[String, String]]
    val metadataColumns = new JArrayList[JMap[String, String]]
    val otherMetadataColumns = new JArrayList[JMap[String, Object]]
    files.foreach {
      file =>
        paths.add(unescapePathName(file.filePath.toString))
        starts.add(JLong.valueOf(file.start))
        lengths.add(JLong.valueOf(file.length))
        val (fileSize, modificationTime) =
          SparkShimLoader.getSparkShims.getFileSizeAndModificationTime(file)
        (fileSize, modificationTime) match {
          case (Some(size), Some(time)) =>
            fileSizes.add(JLong.valueOf(size))
            modificationTimes.add(JLong.valueOf(time))
          case _ => // Do nothing
        }
        val metadataColumn =
          SparkShimLoader.getSparkShims.generateMetadataColumns(file, metadataColumnNames)
        metadataColumns.add(metadataColumn)
        val partitionColumn = new JHashMap[String, String]()
        for (i <- 0 until file.partitionValues.numFields) {
          val partitionColumnValue = if (file.partitionValues.isNullAt(i)) {
            ExternalCatalogUtils.DEFAULT_PARTITION_NAME
          } else {
            val pn = file.partitionValues.get(i, schema.fields(i).dataType)
            schema.fields(i).dataType match {
              case _: BinaryType =>
                new String(pn.asInstanceOf[Array[Byte]], StandardCharsets.UTF_8)
              case _: DateType =>
                DateFormatter.apply().format(pn.asInstanceOf[Integer])
              case _: DecimalType =>
                pn.asInstanceOf[Decimal].toJavaBigInteger.toString
              case _: TimestampType =>
                TimestampFormatter
                  .getFractionFormatter(ZoneOffset.UTC)
                  .format(pn.asInstanceOf[java.lang.Long])
              case _ => pn.toString
            }
          }
          partitionColumn.put(schema.names(i), partitionColumnValue)
        }
        partitionColumns.add(partitionColumn)
        otherMetadataColumns.add(
          SparkShimLoader.getSparkShims.getOtherConstantMetadataColumnValues(file))
    }
    (
      paths,
      starts,
      lengths,
      fileSizes,
      modificationTimes,
      partitionColumns,
      metadataColumns,
      otherMetadataColumns)
  }

  override def injectWriteFilesTempPath(path: String, fileName: String): Unit = {
    NativePlanEvaluator.injectWriteFilesTempPath(path)
  }

  /** Generate Iterator[ColumnarBatch] for first stage. */
  override def genFirstStageIterator(
      inputPartition: BaseGlutenPartition,
      context: TaskContext,
      pipelineTime: SQLMetric,
      updateInputMetrics: InputMetricsWrapper => Unit,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      inputIterators: Seq[Iterator[ColumnarBatch]] = Seq(),
      enableCudf: Boolean = false): Iterator[ColumnarBatch] = {
    assert(
      inputPartition.isInstanceOf[GlutenPartition],
      "Velox backend only accept GlutenPartition.")

    val columnarNativeIterators =
      new JArrayList[ColumnarBatchInIterator](inputIterators.map {
        iter => new ColumnarBatchInIterator(BackendsApiManager.getBackendName, iter.asJava)
      }.asJava)
    val transKernel = NativePlanEvaluator.create(BackendsApiManager.getBackendName)

    val splitInfoByteArray = inputPartition
      .asInstanceOf[GlutenPartition]
      .splitInfosByteArray
    val spillDirPath = SparkDirectoryUtil
      .get()
      .namespace("gluten-spill")
      .mkChildDirRoundRobin(UUID.randomUUID.toString)
      .getAbsolutePath
    val resIter: ColumnarBatchOutIterator =
      transKernel.createKernelWithBatchIterator(
        inputPartition.plan,
        splitInfoByteArray,
        columnarNativeIterators,
        partitionIndex,
        BackendsApiManager.getSparkPlanExecApiInstance.rewriteSpillPath(spillDirPath),
        enableCudf
      )
    val itrMetrics = IteratorMetricsJniWrapper.create()

    Iterators
      .wrap(resIter.asScala)
      .protectInvocationFlow()
      .recycleIterator {
        updateNativeMetrics(itrMetrics.fetch(resIter))
        updateInputMetrics(context.taskMetrics().inputMetrics)
        resIter.close()
      }
      .recyclePayload(batch => batch.close())
      .collectLifeMillis(millis => pipelineTime += millis)
      .asInterruptible(context)
      .create()
  }

  // scalastyle:off argcount

  /** Generate Iterator[ColumnarBatch] for final stage. */
  override def genFinalStageIterator(
      context: TaskContext,
      inputIterators: Seq[Iterator[ColumnarBatch]],
      numaBindingInfo: GlutenNumaBindingInfo,
      sparkConf: SparkConf,
      rootNode: PlanNode,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      materializeInput: Boolean,
      enableCudf: Boolean = false): Iterator[ColumnarBatch] = {

    ExecutorManager.tryTaskSet(numaBindingInfo)

    val transKernel = NativePlanEvaluator.create(BackendsApiManager.getBackendName)
    val columnarNativeIterator =
      new JArrayList[ColumnarBatchInIterator](inputIterators.map {
        iter => new ColumnarBatchInIterator(BackendsApiManager.getBackendName, iter.asJava)
      }.asJava)
    val spillDirPath = SparkDirectoryUtil
      .get()
      .namespace("gluten-spill")
      .mkChildDirRoundRobin(UUID.randomUUID.toString)
      .getAbsolutePath
    val nativeResultIterator =
      transKernel.createKernelWithBatchIterator(
        rootNode.toProtobuf.toByteArray,
        // Final iterator does not contain scan split, so pass empty split info to native here.
        new Array[Array[Byte]](0),
        columnarNativeIterator,
        partitionIndex,
        BackendsApiManager.getSparkPlanExecApiInstance.rewriteSpillPath(spillDirPath),
        enableCudf
      )
    val itrMetrics = IteratorMetricsJniWrapper.create()

    Iterators
      .wrap(nativeResultIterator.asScala)
      .protectInvocationFlow()
      .recycleIterator {
        updateNativeMetrics(itrMetrics.fetch(nativeResultIterator))
        nativeResultIterator.close()
      }
      .recyclePayload(batch => batch.close())
      .collectLifeMillis(millis => pipelineTime += millis)
      .create()
  }
  // scalastyle:on argcount
}

object VeloxIteratorApi {
  // lookup table to translate '0' -> 0 ... 'F'/'f' -> 15
  private val unhexDigits = {
    val array = Array.fill[Byte](128)(-1)
    (0 to 9).foreach(i => array('0' + i) = i.toByte)
    (0 to 5).foreach(i => array('A' + i) = (i + 10).toByte)
    (0 to 5).foreach(i => array('a' + i) = (i + 10).toByte)
    array
  }

  def unescapePathName(path: String): String = {
    if (path == null || path.isEmpty) {
      return path
    }
    var plaintextEndIdx = path.indexOf('%')
    val length = path.length
    if (plaintextEndIdx == -1 || plaintextEndIdx + 2 >= length) {
      // fast path, no %xx encoding found then return the string identity
      path
    } else {
      val sb = new java.lang.StringBuilder(length)
      var plaintextStartIdx = 0
      while (plaintextEndIdx != -1 && plaintextEndIdx + 2 < length) {
        if (plaintextEndIdx > plaintextStartIdx) sb.append(path, plaintextStartIdx, plaintextEndIdx)
        val high = path.charAt(plaintextEndIdx + 1)
        if ((high >>> 8) == 0 && unhexDigits(high) != -1) {
          val low = path.charAt(plaintextEndIdx + 2)
          if ((low >>> 8) == 0 && unhexDigits(low) != -1) {
            sb.append((unhexDigits(high) << 4 | unhexDigits(low)).asInstanceOf[Char])
            plaintextStartIdx = plaintextEndIdx + 3
          } else {
            sb.append('%')
            plaintextStartIdx = plaintextEndIdx + 1
          }
        } else {
          sb.append('%')
          plaintextStartIdx = plaintextEndIdx + 1
        }
        plaintextEndIdx = path.indexOf('%', plaintextStartIdx)
      }
      if (plaintextStartIdx < length) {
        sb.append(path, plaintextStartIdx, length)
      }
      sb.toString
    }
  }
}
