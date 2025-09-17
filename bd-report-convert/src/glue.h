#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum BDReaderErr {
  BDReaderErrCouldNotReadInputFile = -1,
  BDReaderErrCouldNotParseSpec = -2,
  BDReaderErrCouldNotParseInputData = -3,
  BDReaderErrCouldNotValidateOutput = -4,
};

char *bdrc_alloc_json(const char *bin_data_path);
void bdrc_json_free(char *json);

struct Schema {
  const char *data;
  const char *path;
};

uint8_t *bdrc_make_bin_from_json(const struct Schema schemas[],
                                 size_t schema_count,
                                 const char *json_data_path,
                                 int32_t * /* out */ length_or_err);
#ifdef __cplusplus
}; // extern "C"
#endif
