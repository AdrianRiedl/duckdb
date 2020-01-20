#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>
#include <iostream>

using namespace duckdb;
using namespace std;

#define RADIXJOIN_COUNT (size_t) 1024*1024
DUCKDB_BENCHMARK(RadixJoin, "[micro]")
    virtual void Load(DuckDBBenchmarkState *state) {
        // fixed seed random numbers
        std::uniform_int_distribution<> distribution(1, 1410065408);
        std::mt19937 gen;
        gen.seed(42);

        state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
        auto appender = state->conn.OpenAppender(DEFAULT_SCHEMA, "integers"); // insert the elements into the database
        for (size_t i = 0; i < RADIXJOIN_COUNT; i++) {
            appender->BeginRow();
            appender->AppendInteger(distribution(gen));
            appender->AppendInteger(distribution(gen));
            appender->EndRow();
        }
        state->conn.CloseAppender();
    }

    virtual string GetQuery() {
        return "SELECT count(*) FROM integers a, integers b WHERE a.i = b.i;";
    }

    virtual string VerifyResult(QueryResult *result) {
        if (!result->success) {
            return result->error;
        }
        return string();
    }

    virtual string BenchmarkInfo() {
        return StringUtil::Format("Runs the following query: \"SELECT * FROM "
                                  "integers a, integers b WHERE a.i = b.i;\""
                                  " on %d rows",
                                  RADIXJOIN_COUNT);
    }
FINISH_BENCHMARK(RadixJoin)
