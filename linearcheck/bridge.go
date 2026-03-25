package linearcheck

/*
#include "bskiplist.h"
#include <stdlib.h>

#define SCAN_BUF_SIZE 65536

typedef struct {
    bsl_key_t keys[SCAN_BUF_SIZE];
    bsl_val_t vals[SCAN_BUF_SIZE];
    size_t    count;
} scan_result_t;

static void scan_cb(bsl_key_t key, bsl_val_t val, void *arg) {
    scan_result_t *r = (scan_result_t *)arg;
    if (r->count < SCAN_BUF_SIZE) {
        r->keys[r->count] = key;
        r->vals[r->count] = val;
        r->count++;
    }
}

static size_t bsl_scan_collect(bsl_t *list, bsl_key_t start, size_t length,
                                scan_result_t *out) {
    out->count = 0;
    bsl_scan_n(list, start, length, scan_cb, out);
    return out->count;
}
*/
import "C"
import "unsafe"

type BSLMap struct {
	ptr *C.bsl_t
}

func NewBSLMap() *BSLMap {
	return &BSLMap{ptr: C.bsl_new()}
}

func (m *BSLMap) Destroy() {
	C.bsl_destroy(m.ptr)
}

func (m *BSLMap) Insert(key, val uint64) int {
	return int(C.bsl_insert(m.ptr, C.bsl_key_t(key), C.bsl_val_t(val)))
}

func (m *BSLMap) Delete(key uint64) int {
	return int(C.bsl_delete(m.ptr, C.bsl_key_t(key)))
}

func (m *BSLMap) Get(key uint64) (retCode int, val uint64) {
	var outVal C.bsl_val_t
	ret := int(C.bsl_get(m.ptr, C.bsl_key_t(key), &outVal))
	return ret, uint64(outVal)
}

func (m *BSLMap) Scan(start uint64, length int) []KVPair {
	var buf C.scan_result_t
	n := int(C.bsl_scan_collect(m.ptr,
		C.bsl_key_t(start),
		C.size_t(length),
		&buf))

	pairs := make([]KVPair, n)
	keys := (*[1 << 20]C.bsl_key_t)(unsafe.Pointer(&buf.keys[0]))[:n:n]
	vals := (*[1 << 20]C.bsl_val_t)(unsafe.Pointer(&buf.vals[0]))[:n:n]
	for i := range pairs {
		pairs[i] = KVPair{uint64(keys[i]), uint64(vals[i])}
	}
	return pairs
}
