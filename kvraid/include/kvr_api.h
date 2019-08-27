/* kvr_api.h
* 04/23/2019
* by Mian Qin
*/

#ifndef   _kvr_api_h_   
#define   _kvr_api_h_   

#include <stdint.h>

#define KEY_SIZE_BYTES sizeof(uint8_t)
#define VAL_SIZE_BYTES sizeof(uint32_t)

typedef struct {
    char *key;
    uint8_t length;
} kvr_key;

typedef struct {
    char *val;
    uint32_t length;
} kvr_value;


#endif 