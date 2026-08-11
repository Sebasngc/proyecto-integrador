/* =====================================================================
 *  cuda_stubs.cpp — Implementaciones vacías de la API GPU.
 *
 *  Se enlazan cuando se compila SIN nvcc (p. ej. dentro de una imagen de
 *  AWS Lambda estándar, que no expone GPU). Así el mismo binario Python
 *  carga siempre la misma librería y decide en runtime, sin ifdefs en el
 *  código de aplicación: gpu_available() devuelve 0 y el servicio cae
 *  automáticamente al camino OpenMP.
 * ===================================================================== */

#include "normalize.h"
#include <omp.h>
#include <cstdio>
#include <cstring>

extern "C" {

int gpu_available(void) { return 0; }
int gpu_device_count(void) { return 0; }

void gpu_device_name(int /*device*/, char *buf, size_t buflen) {
    if (buf && buflen) std::snprintf(buf, buflen, "sin GPU (build CPU-only)");
}

int gpu_normalize(const float * /*in*/, float * /*out*/, size_t /*n*/, int /*mode*/,
                  int /*streams*/, NormStats * /*stats*/, Timing * /*timing*/) {
    return -3;  /* -3 = "no hay dispositivo": el llamante debe usar la CPU */
}

int omp_max_threads_available(void) { return omp_get_max_threads(); }

const char *normalize_version(void) { return "normalize 1.0.0 (openmp only)"; }

}
