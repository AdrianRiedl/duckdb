add_subdirectory(matcher)
add_subdirectory(join_order)
add_subdirectory(pushdown)
add_subdirectory(rule)

add_library_unity(duckdb_optimizer
                  OBJECT
                  ca_optimizer.cpp
                  cse_optimizer.cpp
                  filter_combiner.cpp
                  filter_pushdown.cpp
                  join_order_optimizer.cpp
                  optimizer.cpp
                  expression_rewriter.cpp
                  regex_range_filter.cpp
                  index_scan.cpp
                  topn_optimizer.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer>
    PARENT_SCOPE)
