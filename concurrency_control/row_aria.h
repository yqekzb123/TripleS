#ifndef ROW_ARIA_H
#define ROW_ARIA_H

#include "row.h"

class Row_aria {
public:
    uint64_t read_reseration;
    uint64_t write_reseration;

    void init(row_t * row);

private:
    row_t * _row;
};

#endif
