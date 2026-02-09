#include <teide/td.h>

/* Element sizes indexed by positive type tag. */
const uint8_t td_type_sizes[TD_TYPE_COUNT] = {
    /* [TD_LIST]      =  0 */ 8,   /* pointer-sized (td_t*) */
    /* [TD_BOOL]      =  1 */ 1,
    /* [TD_U8]        =  2 */ 1,
    /* [TD_CHAR]      =  3 */ 1,
    /* [TD_I16]       =  4 */ 2,
    /* [TD_I32]       =  5 */ 4,
    /* [TD_I64]       =  6 */ 8,
    /* [TD_F64]       =  7 */ 8,
    /* [TD_STR]       =  8 */ 8,   /* pointer-sized (td_t*) */
    /* [TD_DATE]      =  9 */ 4,
    /* [TD_TIME]      = 10 */ 8,
    /* [TD_TIMESTAMP] = 11 */ 8,
    /* [TD_GUID]      = 12 */ 16,
    /* [TD_TABLE]     = 13 */ 8,   /* pointer-sized (td_t*) */
    /* [TD_SYMBOL]    = 14 */ 8,
    /* [TD_ENUM]      = 15 */ 4,
};
