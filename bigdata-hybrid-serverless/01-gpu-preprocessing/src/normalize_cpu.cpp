/* =====================================================================
 *  normalize_cpu.cpp — Implementación de referencia en CPU con OpenMP.
 *
 *  Sirve para tres cosas:
 *   1. Baseline honesto del speedup (mismo algoritmo, misma precisión).
 *   2. Fallback cuando el runtime serverless no tiene GPU visible.
 *   3. Validación numérica de los resultados del kernel CUDA.
 *
 *  Misma estructura de dos pasadas que la versión GPU para que la
 *  comparación mida hardware y no algoritmo.
 * ===================================================================== */

#include "normalize.h"

#include <omp.h>
#include <cmath>
#include <cfloat>
#include <chrono>
#include <algorithm>

static void affine_from_stats_cpu(int mode, const NormStats &s, float *shift, float *scale) {
    const double range = s.max - s.min;
    switch (mode) {
        case NORM_ZSCORE:
            *shift = (float)s.mean;
            *scale = (float)(s.stddev > 1e-12 ? 1.0 / s.stddev : 0.0);
            break;
        case NORM_ROBUST:
            *shift = (float)s.mean;
            *scale = (float)(range > 1e-12 ? 1.0 / range : 0.0);
            break;
        case NORM_MINMAX:
        default:
            *shift = (float)s.min;
            *scale = (float)(range > 1e-12 ? 1.0 / range : 0.0);
            break;
    }
}

extern "C" int cpu_normalize_omp(const float *in, float *out, size_t n, int mode,
                                 int nthreads, NormStats *stats, Timing *timing) {
    if (!in || !out || n == 0) return -2;

    const int threads = nthreads > 0 ? nthreads : omp_get_max_threads();
    const auto t0 = std::chrono::high_resolution_clock::now();

    double gMin = DBL_MAX, gMax = -DBL_MAX, gSum = 0.0, gSq = 0.0;

    /* ---- Pasada 1: reducción -------------------------------------------
     * Las cláusulas reduction(min:)/reduction(max:) requieren OpenMP >= 3.1.
     * schedule(static) porque la carga por iteración es uniforme: evita el
     * sobrecoste de coordinación de dynamic/guided.
     * Acumulamos en double: con float y n=10^8 la suma pierde ~7 dígitos por
     * cancelación catastrófica.
     */
    #pragma omp parallel for num_threads(threads) schedule(static) \
            reduction(min:gMin) reduction(max:gMax) reduction(+:gSum) reduction(+:gSq)
    for (long long i = 0; i < (long long)n; ++i) {
        const double x = (double)in[i];
        if (x < gMin) gMin = x;
        if (x > gMax) gMax = x;
        gSum += x;
        gSq  += x * x;
    }

    NormStats st{};
    st.n      = n;
    st.min    = gMin;
    st.max    = gMax;
    st.sum    = gSum;
    st.sumsq  = gSq;
    st.mean   = gSum / (double)n;
    st.stddev = std::sqrt(std::max(0.0, gSq / (double)n - st.mean * st.mean));

    /* ---- Pasada 2: transformación --------------------------------------- */
    float shift = 0.f, scale = 1.f;
    affine_from_stats_cpu(mode, st, &shift, &scale);

    #pragma omp parallel for num_threads(threads) schedule(static)
    for (long long i = 0; i < (long long)n; ++i) {
        out[i] = (in[i] - shift) * scale;
    }

    const auto t1 = std::chrono::high_resolution_clock::now();
    const double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    if (stats) *stats = st;
    if (timing) {
        timing->h2d_ms    = 0.0;
        timing->d2h_ms    = 0.0;
        timing->kernel_ms = ms;
        timing->total_ms  = ms;
        timing->gbps      = (ms > 0.0) ? (3.0 * n * sizeof(float)) / (ms * 1e6) : 0.0;
    }
    return 0;
}
