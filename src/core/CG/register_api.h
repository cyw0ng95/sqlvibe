#ifndef SVDB_CG_REGISTER_API_H
#define SVDB_CG_REGISTER_API_H

/* C-compatible API header for the CG register allocator and instruction emitter.
 * Used by CGO (which cannot include C++ STL headers from register.h directly). */

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

void*   SVDB_CG_RegisterAllocator_Create(int32_t baseReg);
void    SVDB_CG_RegisterAllocator_Destroy(void* ra);
int32_t SVDB_CG_RegisterAllocator_Alloc(void* ra);
void    SVDB_CG_RegisterAllocator_Free(void* ra, int32_t reg);
void    SVDB_CG_RegisterAllocator_Reset(void* ra);

void*   SVDB_CG_InstrEmitter_Create(void);
void    SVDB_CG_InstrEmitter_Destroy(void* emitter);
void    SVDB_CG_InstrEmitter_Emit(void* emitter, uint16_t op, int32_t p1, int32_t p2, int32_t p3);
int32_t SVDB_CG_InstrEmitter_GetPosition(void* emitter);
void    SVDB_CG_InstrEmitter_Fixup(void* emitter, int32_t pos, int32_t target);
size_t  SVDB_CG_InstrEmitter_GetCount(void* emitter);
void    SVDB_CG_InstrEmitter_GetData(void* emitter, void* outBuf);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_CG_REGISTER_API_H */
