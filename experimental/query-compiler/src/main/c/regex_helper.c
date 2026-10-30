#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <stddef.h>
#include <stdint.h>

// Allocation callbacks are used during preparation/close, never per matched row.
// The Java owner catches callback failures and keeps both callbacks alive until close.
typedef uint64_t (*Alloc)(uint64_t);
typedef void (*Free)(uint64_t, uint64_t);
typedef struct {
    Alloc alloc;
    Free release;
    pcre2_general_context *general;
    pcre2_code *code;
    pcre2_match_data *data;
    pcre2_match_context *context;
    uint32_t group;
} Capture;
typedef union { max_align_t alignment; size_t bytes; } Allocation;

static void *allocate(size_t bytes, void *owner) {
    Capture *capture = owner;
    if (bytes > SIZE_MAX - sizeof(Allocation)) return NULL;
    Allocation *allocation = (Allocation *)(uintptr_t)capture->alloc(bytes + sizeof(Allocation));
    if (!allocation) return NULL;
    allocation->bytes = bytes + sizeof(Allocation);
    return allocation + 1;
}
static void release(void *pointer, void *owner) {
    if (!pointer) return;
    Capture *capture = owner;
    Allocation *allocation = (Allocation *)pointer - 1;
    capture->release((uint64_t)(uintptr_t)allocation, allocation->bytes);
}
void query_capture_free(uint64_t handle) {
    Capture *capture = (Capture *)(uintptr_t)handle;
    if (!capture) return;
    pcre2_match_context_free(capture->context);
    pcre2_match_data_free(capture->data);
    pcre2_code_free(capture->code);
    pcre2_general_context_free(capture->general);
    capture->release(handle, sizeof(Capture));
}
uint64_t query_capture_create(uint64_t pattern, uint64_t size, uint64_t group, uint64_t alloc, uint64_t free_fn) {
    Alloc allocator = (Alloc)(uintptr_t)alloc;
    Capture *capture = (Capture *)(uintptr_t)allocator(sizeof(Capture));
    if (!capture) return 0;
    *capture = (Capture){ .alloc = allocator, .release = (Free)(uintptr_t)free_fn, .group = (uint32_t)group };
    capture->general = pcre2_general_context_create(allocate, release, capture);
    if (!capture->general) goto failure;
    pcre2_compile_context *compile = pcre2_compile_context_create(capture->general);
    if (!compile) goto failure;
    pcre2_set_newline(compile, PCRE2_NEWLINE_ANYCRLF);
    int error;
    PCRE2_SIZE offset;
    capture->code = pcre2_compile((PCRE2_SPTR)(uintptr_t)pattern, size, 0, &error, &offset, compile);
    pcre2_compile_context_free(compile);
    if (!capture->code || pcre2_jit_compile(capture->code, PCRE2_JIT_COMPLETE) != 0) goto failure;
    uint32_t count;
    if (pcre2_pattern_info(capture->code, PCRE2_INFO_CAPTURECOUNT, &count) != 0 || group > count) goto failure;
    capture->data = pcre2_match_data_create_from_pattern(capture->code, capture->general);
    capture->context = pcre2_match_context_create(capture->general);
    if (!capture->data || !capture->context) goto failure;
    pcre2_set_match_limit(capture->context, 65536);
    return (uint64_t)(uintptr_t)capture;
failure:
    query_capture_free((uint64_t)(uintptr_t)capture);
    return 0;
}

// Packed byte offsets; -2 means unchanged input, -3 requests the Java fallback.
int64_t query_capture_match(uint64_t address, uint64_t size, uint64_t handle) {
    Capture *capture = (Capture *)(uintptr_t)handle;
    static const unsigned char empty = 0;
    PCRE2_SPTR subject = size == 0 ? &empty : (PCRE2_SPTR)(uintptr_t)address;
    int result = pcre2_jit_match(capture->code, subject, size, 0, 0, capture->data, capture->context);
    if (result == PCRE2_ERROR_NOMATCH) return -2;
    if (result < 0) return -3;
    PCRE2_SIZE *offsets = pcre2_get_ovector_pointer(capture->data) + 2 * capture->group;
    if (offsets[0] == PCRE2_UNSET) return 0; // A nonparticipating capture is empty.
    return (int64_t)(((uint64_t)(uint32_t)offsets[1] << 32) | (uint32_t)offsets[0]);
}
