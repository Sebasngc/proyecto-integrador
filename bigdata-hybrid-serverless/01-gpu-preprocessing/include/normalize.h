/* normalize.h — API C estable de la librería de preprocesamiento.
 * Se expone con enlace C para poder cargarla desde Python (ctypes) sin
 * depender del name-mangling de C++/nvcc.
 */
#ifndef BIGDATA_NORMALIZE_H
#define BIGDATA_NORMALIZE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Modos de transformación */
#define NORM_MINMAX 0 /* x' = (x - min) / (max - min)        -> [0, 1] */
#define NORM_ZSCORE 1 /* x' = (x - mean) / stddev            -> media 0, sd 1 */
#define NORM_ROBUST 2 /* x' = (x - mean) / (max - min)       -> centrado y escalado */

/* Estadísticos calculados en la pasada 1 */
typedef struct {
    double min;
    double max;
    double mean;
    double stddev;
    double sum;
    double sumsq;
    size_t n;
} NormStats;

/* Desglose de tiempos (ms). En CPU sólo se rellenan kernel_ms y total_ms. */
typedef struct {
    double h2d_ms;    /* host -> device */
    double kernel_ms; /* cómputo puro (pasada 1 + pasada 2) */
    double d2h_ms;    /* device -> host */
    double total_ms;  /* extremo a extremo, incluye asignaciones */
    double gbps;      /* ancho de banda efectivo sobre los datos procesados */
} Timing;

/* ---- Implementación GPU (CUDA + OpenMP para el despacho multi-stream) ---- */
/* Devuelve 0 en éxito, código negativo en error.
 * streams: nº de streams CUDA / hilos OpenMP anfitriones (0 = auto).
 */
int gpu_normalize(const float *in, float *out, size_t n, int mode,
                  int streams, NormStats *stats, Timing *timing);

/* ---- Implementación CPU de referencia (OpenMP) ---- */
/* nthreads: 0 = usar omp_get_max_threads(). */
int cpu_normalize_omp(const float *in, float *out, size_t n, int mode,
                      int nthreads, NormStats *stats, Timing *timing);

/* ---- Utilidades de introspección ---- */
int  gpu_available(void);                 /* 1 si hay al menos una GPU CUDA visible */
int  gpu_device_count(void);
void gpu_device_name(int device, char *buf, size_t buflen);
int  omp_max_threads_available(void);
const char *normalize_version(void);

#ifdef __cplusplus
}
#endif

#endif /* BIGDATA_NORMALIZE_H */
