#include "row_aria.h"

void Row_aria::init(row_t* row) {
    _row = row;
    read_reseration = UINT64_MAX;
    write_reseration = UINT64_MAX;
}
