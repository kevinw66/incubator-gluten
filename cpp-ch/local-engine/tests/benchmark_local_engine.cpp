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
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <Builder/SerializedPlanBuilder.h>
#include <Compression/CompressedReadBuffer.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/LocalExecutor.h>
#include <Parser/ParserContext.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/SparkRowToCHColumn.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Shuffle/ShuffleReader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <benchmark/benchmark.h>
#include <substrait/plan.pb.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>
#include <Common/PODArray_fwd.h>
#include <Common/QueryContext.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include "testConfig.h"

#if defined(__SSE2__)
#include <emmintrin.h>
#endif


using namespace local_engine;
using namespace dbms;
using namespace DB;

DB::ContextMutablePtr global_context;


[[maybe_unused]] static void BM_ParquetRead(benchmark::State & state)
{
    const auto * type_string = "columns format version: 1\n"
                               "2 columns:\n"
                               "`l_returnflag` String\n"
                               "`l_linestatus` String\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    ColumnsWithTypeAndName columns;
    for (const auto & item : names_and_types_list)
    {
        ColumnWithTypeAndName col;
        col.column = item.type->createColumn();
        col.type = item.type;
        col.name = item.name;
        columns.emplace_back(std::move(col));
    }
    auto header = Block(std::move(columns));

    for (auto _ : state)
    {
        substrait::ReadRel::LocalFiles files;
        substrait::ReadRel::LocalFiles::FileOrFiles * file = files.add_items();
        std::string file_path{GLUTEN_SOURCE_TPCH_URI("lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet")};
        file->set_uri_file(file_path);
        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        file->mutable_parquet()->CopyFrom(parquet_format);
        auto builder = std::make_unique<QueryPipelineBuilder>();
        builder->init(Pipe(std::make_shared<SubstraitFileSource>(QueryContext::globalContext(), header, files)));

        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        auto executor = PullingPipelineExecutor(pipeline);
        auto result = header.cloneEmpty();
        size_t total_rows = 0;
        while (executor.pull(result))
        {
            debug::headBlock(result);
            total_rows += result.rows();
        }
        std::cerr << "rows:" << total_rows << std::endl;
    }
}

[[maybe_unused]] static void BM_ShuffleReader(benchmark::State & state)
{
    for (auto _ : state)
    {
        auto read_buffer = std::make_unique<ReadBufferFromFile>("/tmp/test_shuffle/ZSTD/data.dat");
        //        read_buffer->seek(357841655, SEEK_SET);
        auto shuffle_reader = local_engine::ShuffleReader(std::move(read_buffer), true, -1, -1);
        Block * block;
        int sum = 0;
        do
        {
            block = shuffle_reader.read();
            sum += block->rows();
        } while (block->columns() != 0);
        std::cout << "total rows:" << sum << std::endl;
    }
}

[[maybe_unused]] static void BM_SimpleAggregate(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();

        dbms::SerializedSchemaBuilder schema_builder;
        auto * schema = schema_builder.column("l_orderkey", "I64")
                            .column("l_partkey", "I64")
                            .column("l_suppkey", "I64")
                            .column("l_linenumber", "I32")
                            .column("l_quantity", "FP64")
                            .column("l_extendedprice", "FP64")
                            .column("l_discount", "FP64")
                            .column("l_tax", "FP64")
                            .column("l_shipdate_new", "FP64")
                            .column("l_commitdate_new", "FP64")
                            .column("l_receiptdate_new", "FP64")
                            .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto * measure = dbms::measureFunction(dbms::SUM, {dbms::selection(6)});
        auto plan
            = plan_builder.registerSupportedFunctions()
                  .aggregate({}, {measure})
                  .read(
                      "/home/kyligence/Documents/test-dataset/intel-gazelle-test-" + std::to_string(state.range(0)) + ".snappy.parquet",
                      std::move(schema))
                  .build();
        auto parser_context = ParserContext::build(global_context, *plan);
        local_engine::SerializedPlanParser parser(parser_context);
        auto local_executor = parser.createExecutor(*plan);
        state.ResumeTiming();

        while (local_executor->hasNext())
            local_engine::SparkRowInfoPtr spark_row_info = local_executor->next();
    }
}

[[maybe_unused]] static void BM_TPCH_Q6(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto * schema = schema_builder.column("l_discount", "FP64")
                            .column("l_extendedprice", "FP64")
                            .column("l_quantity", "FP64")
                            .column("l_shipdate_new", "Date")
                            .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto * agg_mul = dbms::scalarFunction(dbms::MULTIPLY, {dbms::selection(1), dbms::selection(0)});
        auto * measure1 = dbms::measureFunction(dbms::SUM, {agg_mul});
        auto * measure2 = dbms::measureFunction(dbms::SUM, {dbms::selection(1)});
        auto * measure3 = dbms::measureFunction(dbms::SUM, {dbms::selection(2)});
        auto plan
            = plan_builder.registerSupportedFunctions()
                  .aggregate({}, {measure1, measure2, measure3})
                  .project({dbms::selection(2), dbms::selection(1), dbms::selection(0)})
                  .filter(dbms::scalarFunction(
                      dbms::AND,
                      {dbms::scalarFunction(
                           AND,
                           {dbms::scalarFunction(
                                AND,
                                {dbms::scalarFunction(
                                     AND,
                                     {dbms::scalarFunction(
                                          AND,
                                          {dbms::scalarFunction(
                                               AND,
                                               {dbms::scalarFunction(
                                                    AND,
                                                    {scalarFunction(IS_NOT_NULL, {selection(3)}),
                                                     scalarFunction(IS_NOT_NULL, {selection(0)})}),
                                                scalarFunction(IS_NOT_NULL, {selection(2)})}),
                                           dbms::scalarFunction(GREATER_THAN_OR_EQUAL, {selection(3), literalDate(8766)})}),
                                      scalarFunction(LESS_THAN, {selection(3), literalDate(9131)})}),
                                 scalarFunction(GREATER_THAN_OR_EQUAL, {selection(0), literal(0.05)})}),
                            scalarFunction(LESS_THAN_OR_EQUAL, {selection(0), literal(0.07)})}),
                       scalarFunction(LESS_THAN, {selection(2), literal(24.0)})}))
                  .read(
                      "/home/kyligence/Documents/test-dataset/intel-gazelle-test-" + std::to_string(state.range(0)) + ".snappy.parquet",
                      std::move(schema))
                  .build();
        auto parser_context = ParserContext::build(QueryContext::globalContext(), *plan);
        local_engine::SerializedPlanParser parser(parser_context);
        auto local_executor = parser.createExecutor(*plan);
        state.ResumeTiming();

        while (local_executor->hasNext())
        {
            Block * block = local_executor->nextColumnar();
            delete block;
        }
    }
}

[[maybe_unused]] static void BM_MERGE_TREE_TPCH_Q6_FROM_TEXT(benchmark::State & state)
{
    QueryContext::globalContext() = global_context;
    for (auto _ : state)
    {
        state.PauseTiming();

        //const char * path = "/data1/tpc_data/tpch1000_zhichao/serialized_q6_substrait_plan1.txt";
        const char * path = "/data1/tpc_data/tpch100_zhichao/serialized_q4_substrait_plan_parquet.bin";
        //const char * path = "/data1/tpc_data/tpch100_zhichao/serialized_q4_substrait_plan_mergetree.bin";
        std::ifstream t(path);
        std::string str((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
        std::cout << "the plan from: " << path << std::endl;
        auto plan = BinaryToMessage<substrait::Plan>(str);
        auto parser_context = ParserContext::build(global_context, plan);
        local_engine::SerializedPlanParser parser(parser_context);
        auto local_executor = parser.createExecutor(plan);
        state.ResumeTiming();
        while (local_executor->hasNext()) [[maybe_unused]]
            auto * x = local_executor->nextColumnar();
    }
}


[[maybe_unused]] static void BM_CHColumnToSparkRowWithString(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto * schema = schema_builder.column("l_orderkey", "I64")
                            .column("l_partkey", "I64")
                            .column("l_suppkey", "I64")
                            .column("l_linenumber", "I32")
                            .column("l_quantity", "FP64")
                            .column("l_extendedprice", "FP64")
                            .column("l_discount", "FP64")
                            .column("l_tax", "FP64")
                            .column("l_returnflag", "String")
                            .column("l_linestatus", "String")
                            .column("l_shipdate_new", "FP64")
                            .column("l_commitdate_new", "FP64")
                            .column("l_receiptdate_new", "FP64")
                            .column("l_shipinstruct", "String")
                            .column("l_shipmode", "String")
                            .column("l_comment", "String")
                            .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto plan
            = plan_builder
                  .read(
                      "/home/kyligence/Documents/test-dataset/intel-gazelle-test-" + std::to_string(state.range(0)) + ".snappy.parquet",
                      std::move(schema))
                  .build();
        auto parser_context = ParserContext::build(QueryContext::globalContext(), *plan);
        local_engine::SerializedPlanParser parser(parser_context);

        auto local_executor = parser.createExecutor(*plan);
        state.ResumeTiming();

        while (local_executor->hasNext())
            local_engine::SparkRowInfoPtr spark_row_info = local_executor->next();
    }
}

[[maybe_unused]] static void BM_SparkRowToCHColumn(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto * schema = schema_builder.column("l_orderkey", "I64")
                            .column("l_partkey", "I64")
                            .column("l_suppkey", "I64")
                            .column("l_linenumber", "I32")
                            .column("l_quantity", "FP64")
                            .column("l_extendedprice", "FP64")
                            .column("l_discount", "FP64")
                            .column("l_tax", "FP64")
                            .column("l_shipdate_new", "FP64")
                            .column("l_commitdate_new", "FP64")
                            .column("l_receiptdate_new", "FP64")
                            .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto plan
            = plan_builder
                  .read(
                      "/home/kyligence/Documents/test-dataset/intel-gazelle-test-" + std::to_string(state.range(0)) + ".snappy.parquet",
                      std::move(schema))
                  .build();

        auto parser_context = ParserContext::build(QueryContext::globalContext(), *plan);
        local_engine::SerializedPlanParser parser(parser_context);
        auto local_executor = parser.createExecutor(*plan);
        local_engine::SparkRowToCHColumn converter;
        while (local_executor->hasNext())
        {
            local_engine::SparkRowInfoPtr spark_row_info = local_executor->next();
            state.ResumeTiming();
            auto block = converter.convertSparkRowInfoToCHColumn(*spark_row_info, local_executor->getHeader());
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
}


[[maybe_unused]] static void BM_SparkRowToCHColumnWithString(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        dbms::SerializedSchemaBuilder schema_builder;
        auto * schema = schema_builder.column("l_orderkey", "I64")
                            .column("l_partkey", "I64")
                            .column("l_suppkey", "I64")
                            .column("l_linenumber", "I32")
                            .column("l_quantity", "FP64")
                            .column("l_extendedprice", "FP64")
                            .column("l_discount", "FP64")
                            .column("l_tax", "FP64")
                            .column("l_returnflag", "String")
                            .column("l_linestatus", "String")
                            .column("l_shipdate_new", "FP64")
                            .column("l_commitdate_new", "FP64")
                            .column("l_receiptdate_new", "FP64")
                            .column("l_shipinstruct", "String")
                            .column("l_shipmode", "String")
                            .column("l_comment", "String")
                            .build();
        dbms::SerializedPlanBuilder plan_builder;
        auto plan
            = plan_builder
                  .read(
                      "/home/kyligence/Documents/test-dataset/intel-gazelle-test-" + std::to_string(state.range(0)) + ".snappy.parquet",
                      std::move(schema))
                  .build();
        auto parser_context = ParserContext::build(QueryContext::globalContext(), *plan);
        local_engine::SerializedPlanParser parser(parser_context);
        auto local_executor = parser.createExecutor(*plan);
        local_engine::SparkRowToCHColumn converter;
        while (local_executor->hasNext())
        {
            local_engine::SparkRowInfoPtr spark_row_info = local_executor->next();
            state.ResumeTiming();
            auto block = converter.convertSparkRowInfoToCHColumn(*spark_row_info, local_executor->getHeader());
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
}

[[maybe_unused]] static void BM_SIMDFilter(benchmark::State & state)
{
    const int n = 10000000;
    for (auto _ : state)
    {
        state.PauseTiming();
        PaddedPODArray<Int32> arr;
        PaddedPODArray<UInt8> condition;
        PaddedPODArray<Int32> res_data;
        arr.reserve_exact(n);
        condition.reserve_exact(n);
        res_data.reserve_exact(n);
        for (int i = 0; i < n; i++)
        {
            arr.push_back(i);
            condition.push_back(state.range(0));
        }
        const Int32 * data_pos = arr.data();
        const UInt8 * filt_pos = condition.data();
        state.ResumeTiming();
#ifdef __SSE2__
        int size = n;
        static constexpr size_t SIMD_BYTES = 16;
        const __m128i zero16 = _mm_setzero_si128();
        const UInt8 * filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

        while (filt_pos < filt_end_sse)
        {
            UInt16 mask = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));
            mask = ~mask;

            if (0 == mask)
            {
                /// Nothing is inserted.
            }
            else if (0xFFFF == mask)
            {
                res_data.insert(data_pos, data_pos + SIMD_BYTES);
            }
            else
            {
                for (size_t i = 0; i < SIMD_BYTES; ++i)
                    if (filt_pos[i]) [[maybe_unused]]
                        auto x = data_pos[i];
            }

            filt_pos += SIMD_BYTES;
            data_pos += SIMD_BYTES;
        }
#endif
    }
}

[[maybe_unused]] static void BM_NormalFilter(benchmark::State & state)
{
    const int n = 10000000;
    for (auto _ : state)
    {
        state.PauseTiming();
        PaddedPODArray<Int32> arr;
        PaddedPODArray<UInt8> condition;
        PaddedPODArray<Int32> res_data;
        arr.reserve_exact(n);
        condition.reserve_exact(n);
        res_data.reserve_exact(n);
        for (int i = 0; i < n; i++)
        {
            arr.push_back(i);
            condition.push_back(state.range(0));
        }
        const Int32 * data_pos = arr.data();
        const UInt8 * filt_pos = condition.data();
        const UInt8 * filt_end = filt_pos + n;
        state.ResumeTiming();
        while (filt_pos < filt_end)
        {
            if (*filt_pos)
                res_data.push_back(*data_pos);

            ++filt_pos;
            ++data_pos;
        }
    }
}

[[maybe_unused]] static int add(int a, int b)
{
    return a + b;
}

[[maybe_unused]] static void BM_TestSum(benchmark::State & state)
{
    int cnt = state.range(0);
    int i = 0;
    std::vector<int> x;
    std::vector<int> y;
    x.reserve(cnt);
    x.assign(cnt, 2);
    y.reserve(cnt);

    for (auto _ : state)
        for (i = 0; i < cnt; i++)
            y[i] = add(x[i], i);
}

[[maybe_unused]] static void BM_TestSumInline(benchmark::State & state)
{
    int cnt = state.range(0);
    int i = 0;
    std::vector<int> x;
    std::vector<int> y;
    x.reserve(cnt);
    x.assign(cnt, 2);
    y.reserve(cnt);

    for (auto _ : state)
        for (i = 0; i < cnt; i++)
            y[i] = x[i] + i;
}

[[maybe_unused]] static void BM_TestPlus(benchmark::State & state)
{
    UInt64 rows = state.range(0);
    auto & factory = FunctionFactory::instance();
    auto & type_factory = DataTypeFactory::instance();
    auto plus = factory.get("plus", global_context);
    auto type = type_factory.get("UInt64");
    ColumnsWithTypeAndName arguments;
    arguments.push_back(ColumnWithTypeAndName(type, "x"));
    arguments.push_back(ColumnWithTypeAndName(type, "y"));
    auto function = plus->build(arguments);

    ColumnsWithTypeAndName arguments_with_data;
    Block block;
    auto x = ColumnWithTypeAndName(type, "x");
    auto y = ColumnWithTypeAndName(type, "y");
    MutableColumnPtr mutable_x = x.type->createColumn();
    MutableColumnPtr mutable_y = y.type->createColumn();
    mutable_x->reserve(rows);
    mutable_y->reserve(rows);
    ColumnVector<UInt64> & column_x = assert_cast<ColumnVector<UInt64> &>(*mutable_x);
    ColumnVector<UInt64> & column_y = assert_cast<ColumnVector<UInt64> &>(*mutable_y);
    for (UInt64 i = 0; i < rows; i++)
    {
        column_x.insertValue(i);
        column_y.insertValue(i + 1);
    }
    x.column = std::move(mutable_x);
    y.column = std::move(mutable_y);
    block.insert(x);
    block.insert(y);
    auto executable_function = function->prepare(arguments);
    for (auto _ : state)
        auto result = executable_function->execute(block.getColumnsWithTypeAndName(), type, rows, false);
}

[[maybe_unused]] static void BM_TestPlusEmbedded(benchmark::State & state)
{
    UInt64 rows = state.range(0);
    auto & factory = FunctionFactory::instance();
    auto & type_factory = DataTypeFactory::instance();
    auto plus = factory.get("plus", global_context);
    auto type = type_factory.get("UInt64");
    ColumnsWithTypeAndName arguments;
    arguments.push_back(ColumnWithTypeAndName(type, "x"));
    arguments.push_back(ColumnWithTypeAndName(type, "y"));
    auto function = plus->build(arguments);
    ColumnsWithTypeAndName arguments_with_data;
    Block block;
    auto x = ColumnWithTypeAndName(type, "x");
    auto y = ColumnWithTypeAndName(type, "y");
    MutableColumnPtr mutable_x = x.type->createColumn();
    MutableColumnPtr mutable_y = y.type->createColumn();
    mutable_x->reserve(rows);
    mutable_y->reserve(rows);
    ColumnVector<UInt64> & column_x = assert_cast<ColumnVector<UInt64> &>(*mutable_x);
    ColumnVector<UInt64> & column_y = assert_cast<ColumnVector<UInt64> &>(*mutable_y);
    for (UInt64 i = 0; i < rows; i++)
    {
        column_x.insertValue(i);
        column_y.insertValue(i + 1);
    }
    x.column = std::move(mutable_x);
    y.column = std::move(mutable_y);
    block.insert(x);
    block.insert(y);
    CHJIT chjit;
    auto compiled_function = compileFunction(chjit, *function);
    std::vector<ColumnData> columns(arguments.size() + 1);
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        auto column = block.getByPosition(i).column->convertToFullIfNeeded();
        columns[i] = getColumnData(column.get());
    }
    for (auto _ : state)
    {
        auto result_column = type->createColumn();
        result_column->reserve(rows);
        columns[arguments.size()] = getColumnData(result_column.get());
        compiled_function.compiled_function(rows, columns.data());
    }
}

[[maybe_unused]] static void BM_TestReadColumn(benchmark::State & state)
{
    for (auto _ : state)
    {
        ReadBufferFromFile data_buf("/home/saber/Documents/test/c151.bin", 100000);
        CompressedReadBuffer compressed(data_buf);
        ReadBufferFromFile buf("/home/saber/Documents/test/c151.mrk2");
        while (!buf.eof() && !data_buf.eof())
        {
            size_t x;
            size_t y;
            size_t z;
            readIntBinary(x, buf);
            readIntBinary(y, buf);
            readIntBinary(z, buf);
            std::cout << std::to_string(x) + " " << std::to_string(y) + " " << std::to_string(z) + " " << "\n";
            data_buf.seek(x, SEEK_SET);
            assert(!data_buf.eof());
            std::string data;
            data.reserve(y);
            compressed.readBig(reinterpret_cast<char *>(data.data()), y);
            std::cout << data << "\n";
        }
    }
}

[[maybe_unused]] static double quantile(const std::vector<double> & x)
{
    double q = 0.8;
    assert(q >= 0.0 && q <= 1.0);
    const int n = x.size();
    double id = (n - 1) * q;
    int lo = static_cast<int>(floor(id));
    int hi = static_cast<int>(ceil(id));
    double qs = x[lo];
    double h = (id - lo);
    return (1.0 - h) * qs + h * x[hi];
}

// compress benchmark
#include <cstring>
#include <optional>
#include <base/types.h>

#include <Compression/CompressionInfo.h>
#include <Compression/LZ4_decompress_faster.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <base/unaligned.h>
#include <Common/PODArray.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/memcpySmall.h>
namespace DB
{
class FasterCompressedReadBufferBase
{
protected:
    ReadBuffer * compressed_in;

    /// If 'compressed_in' buffer has whole compressed block - then use it. Otherwise copy parts of data to 'own_compressed_buffer'.
    PODArray<char> own_compressed_buffer;
    /// Points to memory, holding compressed block.
    char * compressed_buffer = nullptr;

    ssize_t variant;

    /// Variant for reference implementation of LZ4.
    static constexpr ssize_t LZ4_REFERENCE = -3;

    LZ4::StreamStatistics stream_stat;
    LZ4::PerformanceStatistics perf_stat;

    size_t readCompressedData(size_t & size_decompressed, size_t & size_compressed_without_checksum)
    {
        if (compressed_in->eof())
            return 0;

        CityHash_v1_0_2::uint128 checksum;
        compressed_in->readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

        own_compressed_buffer.resize(COMPRESSED_BLOCK_HEADER_SIZE);
        compressed_in->readStrict(own_compressed_buffer.data(), COMPRESSED_BLOCK_HEADER_SIZE);

        UInt8 method = own_compressed_buffer[0]; /// See CompressedWriteBuffer.h

        size_t & size_compressed = size_compressed_without_checksum;

        if (method == static_cast<UInt8>(CompressionMethodByte::LZ4) || method == static_cast<UInt8>(CompressionMethodByte::ZSTD)
            || method == static_cast<UInt8>(CompressionMethodByte::NONE))
        {
            size_compressed = unalignedLoad<UInt32>(&own_compressed_buffer[1]);
            size_decompressed = unalignedLoad<UInt32>(&own_compressed_buffer[5]);
        }
        else
            throw std::runtime_error("Unknown compression method: " + toString(method));

        if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
            throw std::runtime_error("Too large size_compressed. Most likely corrupted data.");

        /// Is whole compressed block located in 'compressed_in' buffer?
        if (compressed_in->offset() >= COMPRESSED_BLOCK_HEADER_SIZE
            && compressed_in->position() + size_compressed - COMPRESSED_BLOCK_HEADER_SIZE <= compressed_in->buffer().end())
        {
            compressed_in->position() -= COMPRESSED_BLOCK_HEADER_SIZE;
            compressed_buffer = compressed_in->position();
            compressed_in->position() += size_compressed;
        }
        else
        {
            own_compressed_buffer.resize(size_compressed + (variant == LZ4_REFERENCE ? 0 : LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER));
            compressed_buffer = own_compressed_buffer.data();
            compressed_in->readStrict(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
        }

        return size_compressed + sizeof(checksum);
    }

    void decompress(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
    {
        UInt8 method = compressed_buffer[0]; /// See CompressedWriteBuffer.h

        if (method == static_cast<UInt8>(CompressionMethodByte::LZ4))
        {
            //LZ4::statistics(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_decompressed, stat);
            LZ4::decompress(
                compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_compressed_without_checksum, size_decompressed, perf_stat);
        }
        else
            throw std::runtime_error("Unknown compression method: " + toString(method));
    }

public:
    /// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
    FasterCompressedReadBufferBase(ReadBuffer * in, ssize_t variant_)
        : compressed_in(in), own_compressed_buffer(COMPRESSED_BLOCK_HEADER_SIZE), variant(variant_), perf_stat(variant)
    {
    }
    LZ4::StreamStatistics getStreamStatistics() const { return stream_stat; }
    LZ4::PerformanceStatistics getPerformanceStatistics() const { return perf_stat; }
};


class FasterCompressedReadBuffer : public FasterCompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
    size_t size_compressed = 0;

    bool nextImpl() override
    {
        size_t size_decompressed;
        size_t size_compressed_without_checksum;
        size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum);
        if (!size_compressed)
            return false;

        memory.resize(size_decompressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
        working_buffer = Buffer(memory.data(), &memory[size_decompressed]);

        decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

        return true;
    }

public:
    FasterCompressedReadBuffer(ReadBuffer & in_, ssize_t method)
        : FasterCompressedReadBufferBase(&in_, method), BufferWithOwnMemory<ReadBuffer>(0)
    {
    }
};

}


[[maybe_unused]] static void BM_TestDecompress(benchmark::State & state)
{
    std::vector<String> files
        = {"/home/saber/Documents/data/mergetree/all_1_1_0/l_discount.bin",
           "/home/saber/Documents/data/mergetree/all_1_1_0/l_extendedprice.bin",
           "/home/saber/Documents/data/mergetree/all_1_1_0/l_quantity.bin",
           "/home/saber/Documents/data/mergetree/all_1_1_0/l_shipdate.bin",

           "/home/saber/Documents/data/mergetree/all_2_2_0/l_discount.bin",
           "/home/saber/Documents/data/mergetree/all_2_2_0/l_extendedprice.bin",
           "/home/saber/Documents/data/mergetree/all_2_2_0/l_quantity.bin",
           "/home/saber/Documents/data/mergetree/all_2_2_0/l_shipdate.bin",

           "/home/saber/Documents/data/mergetree/all_3_3_0/l_discount.bin",
           "/home/saber/Documents/data/mergetree/all_3_3_0/l_extendedprice.bin",
           "/home/saber/Documents/data/mergetree/all_3_3_0/l_quantity.bin",
           "/home/saber/Documents/data/mergetree/all_3_3_0/l_shipdate.bin"};
    for (auto _ : state)
    {
        for (const auto & file : files)
        {
            ReadBufferFromFile in(file);
            FasterCompressedReadBuffer decompressing_in(in, state.range(0));
            while (!decompressing_in.eof())
            {
                decompressing_in.position() = decompressing_in.buffer().end();
                decompressing_in.next();
            }
            // std::cout << "call count:" << std::to_string(decompressing_in.getPerformanceStatistics().data[state.range(0)].count) << "\n";
            // std::cout << "false count:" << std::to_string(decompressing_in.false_count) << "\n";
            // decompressing_in.getStreamStatistics().print();
        }
    }
}

#include <Parser/CHColumnToSparkRow.h>

struct MergeTreeWithSnapshot
{
    std::shared_ptr<local_engine::SparkStorageMergeTree> merge_tree;
    std::shared_ptr<StorageSnapshot> snapshot;
    NamesAndTypesList columns;
};

QueryPlanPtr readFromMergeTree(MergeTreeWithSnapshot storage)
{
    auto query_info = local_engine::buildQueryInfo(storage.columns);
    auto data_parts = storage.merge_tree->getDataPartsVectorForInternalUsage();
    auto query_plan = std::make_unique<QueryPlan>();
    auto step = storage.merge_tree->reader.readFromParts(
        RangesInDataParts{data_parts}, {}, storage.columns.getNames(), storage.snapshot, *query_info, global_context, 10000, 1);
    query_plan->addStep(std::move(step));
    return query_plan;
}

QueryPlanPtr joinPlan(QueryPlanPtr left, QueryPlanPtr right, String left_key, String right_key)
{
    auto join = std::make_shared<TableJoin>(
        global_context->getSettingsRef(), global_context->getGlobalTemporaryVolume(), global_context->getTempDataOnDisk());
    auto left_columns = left->getCurrentHeader().getColumnsWithTypeAndName();
    auto right_columns = right->getCurrentHeader().getColumnsWithTypeAndName();
    join->setKind(JoinKind::Left);
    join->setStrictness(JoinStrictness::All);
    join->setColumnsFromJoinedTable(right->getCurrentHeader().getNamesAndTypesList());
    join->addDisjunct();
    ASTPtr lkey = std::make_shared<ASTIdentifier>(left_key);
    ASTPtr rkey = std::make_shared<ASTIdentifier>(right_key);
    join->addOnKeys(lkey, rkey, true);
    for (const auto & column : join->columnsFromJoinedTable())
        join->addJoinedColumn(column);

    auto left_keys = left->getCurrentHeader().getNamesAndTypesList();
    join->addJoinedColumnsAndCorrectTypes(left_keys, true);
    std::optional<ActionsDAG> left_convert_actions;
    std::optional<ActionsDAG> right_convert_actions;
    std::tie(left_convert_actions, right_convert_actions) = join->createConvertingActions(left_columns, right_columns);

    if (right_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right->getCurrentHeader(), std::move(*right_convert_actions));
        converting_step->setStepDescription("Convert joined columns");
        right->addStep(std::move(converting_step));
    }

    if (left_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right->getCurrentHeader(), std::move(*right_convert_actions));
        converting_step->setStepDescription("Convert joined columns");
        left->addStep(std::move(converting_step));
    }
    auto hash_join = std::make_shared<HashJoin>(join, right->getCurrentHeader());

    QueryPlanStepPtr join_step = std::make_unique<JoinStep>(
        left->getCurrentHeader(), right->getCurrentHeader(), hash_join, DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE, 524288, 1, NameSet{}, false, false);

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::move(left));
    plans.emplace_back(std::move(right));

    auto query_plan = std::make_unique<QueryPlan>();
    query_plan->unitePlans(std::move(join_step), std::move(plans));
    return query_plan;
}

BENCHMARK(BM_ParquetRead)->Unit(benchmark::kMillisecond)->Iterations(10);

// BENCHMARK(BM_TestDecompress)->Arg(0)->Arg(1)->Arg(2)->Arg(3)->Unit(benchmark::kMillisecond)->Iterations(50)->Repetitions(6)->ComputeStatistics("80%", quantile);
// BENCHMARK(BM_JoinTest)->Unit(benchmark::k
// Millisecond)->Iterations(10)->Repetitions(250)->ComputeStatistics("80%", quantile);

//BENCHMARK(BM_CHColumnToSparkRow)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_MergeTreeRead)->Arg(1)->Unit(benchmark::kMillisecond)->Iterations(10);

//BENCHMARK(BM_ShuffleSplitter)->Args({2, 0})->Args({2, 1})->Args({2, 2})->Unit(benchmark::kMillisecond)->Iterations(1);
//BENCHMARK(BM_HashShuffleSplitter)->Args({2, 0})->Args({2, 1})->Args({2, 2})->Unit(benchmark::kMillisecond)->Iterations(1);
//BENCHMARK(BM_ShuffleReader)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SimpleAggregate)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_SIMDFilter)->Arg(1)->Arg(0)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_NormalFilter)->Arg(1)->Arg(0)->Unit(benchmark::kMillisecond)->Iterations(40);
//BENCHMARK(BM_TPCH_Q6)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_MERGE_TREE_TPCH_Q6)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_MERGE_TREE_TPCH_Q6_NEW)->Unit(benchmark::kMillisecond)->Iterations(10);

//BENCHMARK(BM_MERGE_TREE_TPCH_Q6_FROM_TEXT)->Unit(benchmark::kMillisecond)->Iterations(5);

//BENCHMARK(BM_CHColumnToSparkRowWithString)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SparkRowToCHColumn)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_SparkRowToCHColumnWithString)->Arg(1)->Arg(3)->Arg(30)->Arg(90)->Arg(150)->Unit(benchmark::kMillisecond)->Iterations(10);
//BENCHMARK(BM_TestCreateExecute)->Unit(benchmark::kMillisecond)->Iterations(1000);
//BENCHMARK(BM_TestReadColumn)->Unit(benchmark::kMillisecond)->Iterations(1);

//BENCHMARK(BM_TestSum)->Arg(1000000)->Unit(benchmark::kMicrosecond)->Iterations(100)->Repetitions(100)->ComputeStatistics("80%", quantile)->DisplayAggregatesOnly();
//BENCHMARK(BM_TestSumInline)->Arg(1000000)->Unit(benchmark::kMicrosecond)->Iterations(100)->Repetitions(100)->ComputeStatistics("80%", quantile)->DisplayAggregatesOnly();
//
//BENCHMARK(BM_TestPlus)->Arg(65505)->Unit(benchmark::kMicrosecond)->Iterations(100)->Repetitions(1000)->ComputeStatistics("80%", quantile)->DisplayAggregatesOnly();
//BENCHMARK(BM_TestPlusEmbedded)->Arg(65505)->Unit(benchmark::kMicrosecond)->Iterations(100)->Repetitions(1000)->ComputeStatistics("80%", quantile)->DisplayAggregatesOnly();


int main(int argc, char ** argv)
{
    SparkConfigs::ConfigMap empty;
    BackendInitializerUtil::initBackend(empty);
    SCOPE_EXIT({ BackendFinalizerUtil::finalizeGlobally(); });

    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv))
        return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
