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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression._
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.{Final, Partial, WindowGroupLimitMode}

import com.google.protobuf.StringValue
import io.substrait.proto.SortField

import scala.collection.JavaConverters._

case class CHWindowGroupLimitExecTransformer(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan)
  extends UnaryTransformSupport {

  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genWindowTransformerMetrics(sparkContext)

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genWindowTransformerMetricsUpdater(metrics)

  override def output: Seq[Attribute] = child.output

  override def requiredChildDistribution: Seq[Distribution] = mode match {
    case Partial => super.requiredChildDistribution
    case Final =>
      if (partitionSpec.isEmpty) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(partitionSpec) :: Nil
      }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    if (BackendsApiManager.getSettings.requiredChildOrderingForWindowGroupLimit()) {
      Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
    } else {
      Seq(Nil)
    }
  }

  override def outputOrdering: Seq[SortOrder] = {
    if (requiredChildOrdering.forall(_.isEmpty)) {
      // The Velox backend `TopNRowNumber` does not require child ordering, because it
      // uses hash table to store partition and use priority queue to track of top limit rows.
      // Ideally, the output of `TopNRowNumber` is unordered but it is grouped for partition keys.
      // To be safe, here we do not propagate the ordering.
      // TODO: Make the framework aware of grouped data distribution
      Nil
    } else {
      child.outputOrdering
    }
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  def getWindowGroupLimitRel(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    // Partition By Expressions
    val partitionsExpressions = partitionSpec
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, attributeSeq = child.output)
          .doTransform(context))
      .asJava

    // Sort By Expressions
    val sortFieldList =
      orderSpec.map {
        order =>
          val builder = SortField.newBuilder()
          val exprNode = ExpressionConverter
            .replaceWithExpressionTransformer(order.child, attributeSeq = child.output)
            .doTransform(context)
          builder.setExpr(exprNode.toProtobuf)
          builder.setDirectionValue(SortExecTransformer.transformSortDirection(order))
          builder.build()
      }.asJava
    if (!validation) {
      val windowFunction = rankLikeFunction match {
        case _: RowNumber => ExpressionNames.ROW_NUMBER
        case _: Rank => ExpressionNames.RANK
        case _: DenseRank => ExpressionNames.DENSE_RANK
        case _ => throw new GlutenNotSupportException(s"Unknow window function $rankLikeFunction")
      }
      val parametersStr = new StringBuffer("WindowGroupLimitParameters:")
      parametersStr
        .append("window_function=")
        .append(windowFunction)
        .append("\n")
      val message = StringValue.newBuilder().setValue(parametersStr.toString).build()
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(message),
        null)
      RelBuilder.makeWindowGroupLimitRel(
        input,
        partitionsExpressions,
        sortFieldList,
        limit,
        extensionNode,
        context,
        operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))

      RelBuilder.makeWindowGroupLimitRel(
        input,
        partitionsExpressions,
        sortFieldList,
        limit,
        extensionNode,
        context,
        operatorId)
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!BackendsApiManager.getSettings.supportWindowGroupLimitExec(rankLikeFunction)) {
      return ValidationResult
        .failed(s"Found unsupported rank like function: $rankLikeFunction")
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode =
      getWindowGroupLimitRel(substraitContext, child.output, operatorId, null, validation = true)

    doNativeValidation(substraitContext, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)

    val currRel =
      getWindowGroupLimitRel(context, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Window Group Limit Rel should be valid")
    TransformContext(output, currRel)
  }
}
