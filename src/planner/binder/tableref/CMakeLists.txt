add_library_unity(duckdb_bind_tableref
                  OBJECT
                  bind_basetableref.cpp
                  bind_crossproductref.cpp
                  bind_emptytableref.cpp
                  bind_joinref.cpp
                  bind_subqueryref.cpp
                  bind_table_function.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_bind_tableref>
    PARENT_SCOPE)
