/* =====================================================================
 *  normalize.cu — Preprocesamiento numérico en GPU (CUDA) con despacho
 *  anfitrión paralelo (OpenMP).
 *
 *  Estrategia: la normalización es una operación *global* (necesita min,
 *  max, media y desviación de TODO el array antes de transformar), así que
 *  se resuelve en dos pasadas:
 *
 *    Pasada 1 — reducción: cada bloque calcula (min, max, sum, sumsq) de su
 *               porción mediante grid-stride loop + reducción warp-level con
 *               __shfl_down_sync (sin bank conflicts, sin __syncthreads en
 *               el nivel de warp) y vuelca un parcial por bloque.
 *               La reducción final entre bloques (pocos cientos de valores)
 *               se hace en CPU: es O(nBlocks) y evita atómicos costosos y
 *               no deterministas en punto flotante.
 *
 *    Pasada 2 — transformación: mapa elemento a elemento, fusionado en una
 *               sola operación afín  out = (x - a) * inv_b, de forma que los
 *               tres modos (minmax/zscore/robust) comparten el mismo kernel.
 *
 *  OpenMP se usa en dos sitios:
 *    (a) para que cada hilo anfitrión gestione su propio stream CUDA y
 *        solape H2D / kernel / D2H (pipelining de chunks);
 *    (b) para la implementación CPU de referencia (normalize_cpu.cpp).
 *
 *  Compilar:
 *    nvcc -O3 -Xcompiler -fopenmp -arch=sm_75 --shared -Xcompiler -fPIC \
 *         -Iinclude src/normalize.cu src/normalize_cpu.cpp -o libnormalize.so
 * ===================================================================== */

#include "normalize.h"

#include <cuda_runtime.h>
#include <omp.h>

#include <cstdio>
#include <cfloat>
#include <cmath>
#include <chrono>
#include <vector>
#include <algorithm>
#include <cstring>

#define CUDA_TRY(call)                                                        \
    do {                                                                      \
        cudaError_t _err = (call);                                            \
        if (_err != cudaSuccess) {                                            \
            std::fprintf(stderr, "[CUDA] %s:%d %s -> %s\n", __FILE__,         \
                         __LINE__, #call, cudaGetErrorString(_err));          \
            return -1;                                                        \
        }                                                                     \
    } while (0)

static const int kBlockSize   = 256;
static const int kMaxBlocks   = 1024;   /* parciales que reduce la CPU */
static const int kWarpSize    = 32;

/* ------------------------- reducciones warp/bloque ------------------------ */

__inline__ __device__ float warpReduceMin(float v) {
    for (int off = kWarpSize / 2; off > 0; off >>= 1)
        v = fminf(v, __shfl_down_sync(0xffffffffu, v, off));
    return v;
}
__inline__ __device__ float warpReduceMax(float v) {
    for (int off = kWarpSize / 2; off > 0; off >>= 1)
        v = fmaxf(v, __shfl_down_sync(0xffffffffu, v, off));
    return v;
}
__inline__ __device__ double warpReduceSum(double v) {
    for (int off = kWarpSize / 2; off > 0; off >>= 1)
        v += __shfl_down_sync(0xffffffffu, v, off);
    return v;
}

/* ------------------------------ pasada 1 --------------------------------- */
/* Un parcial por bloque en cada uno de los 4 arrays de salida. */
__global__ void statsKernel(const float *__restrict__ in, size_t n,
                            float *__restrict__ bMin, float *__restrict__ bMax,
                            double *__restrict__ bSum, double *__restrict__ bSumSq) {
    __shared__ float  sMin[kWarpSize];
    __shared__ float  sMax[kWarpSize];
    __shared__ double sSum[kWarpSize];
    __shared__ double sSq[kWarpSize];

    float  vMin = FLT_MAX, vMax = -FLT_MAX;
    double vSum = 0.0, vSq = 0.0;

    /* grid-stride loop: accesos coalescidos y desacoplados del tamaño del grid */
    for (size_t i = blockIdx.x * (size_t)blockDim.x + threadIdx.x;
         i < n; i += (size_t)blockDim.x * gridDim.x) {
        const float x = in[i];
        vMin = fminf(vMin, x);
        vMax = fmaxf(vMax, x);
        const double xd = (double)x;      /* acumular en doble evita cancelación */
        vSum += xd;
        vSq  += xd * xd;
    }

    const int lane = threadIdx.x % kWarpSize;
    const int warp = threadIdx.x / kWarpSize;

    vMin = warpReduceMin(vMin);
    vMax = warpReduceMax(vMax);
    vSum = warpReduceSum(vSum);
    vSq  = warpReduceSum(vSq);

    if (lane == 0) { sMin[warp] = vMin; sMax[warp] = vMax; sSum[warp] = vSum; sSq[warp] = vSq; }
    __syncthreads();

    const int nWarps = (blockDim.x + kWarpSize - 1) / kWarpSize;
    if (warp == 0) {
        vMin = (lane < nWarps) ? sMin[lane] :  FLT_MAX;
        vMax = (lane < nWarps) ? sMax[lane] : -FLT_MAX;
        vSum = (lane < nWarps) ? sSum[lane] : 0.0;
        vSq  = (lane < nWarps) ? sSq[lane]  : 0.0;

        vMin = warpReduceMin(vMin);
        vMax = warpReduceMax(vMax);
        vSum = warpReduceSum(vSum);
        vSq  = warpReduceSum(vSq);

        if (lane == 0) {
            bMin[blockIdx.x]   = vMin;
            bMax[blockIdx.x]   = vMax;
            bSum[blockIdx.x]   = vSum;
            bSumSq[blockIdx.x] = vSq;
        }
    }
}

/* ------------------------------ pasada 2 --------------------------------- */
/* out = (x - shift) * scale. Un único kernel para los tres modos: el modo se
 * resuelve en CPU eligiendo (shift, scale), evitando divergencia en el kernel. */
__global__ void applyKernel(const float *__restrict__ in, float *__restrict__ out,
                            size_t n, float shift, float scale) {
    for (size_t i = blockIdx.x * (size_t)blockDim.x + threadIdx.x;
         i < n; i += (size_t)blockDim.x * gridDim.x) {
        out[i] = (in[i] - shift) * scale;
    }
}

/* ------------------------------ utilidades ------------------------------- */

extern "C" int gpu_available(void) {
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess) return 0;
    return count > 0 ? 1 : 0;
}

extern "C" int gpu_device_count(void) {
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess) return 0;
    return count;
}

extern "C" void gpu_device_name(int device, char *buf, size_t buflen) {
    if (!buf || buflen == 0) return;
    cudaDeviceProp prop;
    if (cudaGetDeviceProperties(&prop, device) == cudaSuccess) {
        std::snprintf(buf, buflen, "%s (sm_%d%d, %d SM, %.1f GB)", prop.name,
                      prop.major, prop.minor, prop.multiProcessorCount,
                      prop.totalGlobalMem / (1024.0 * 1024.0 * 1024.0));
    } else {
        std::snprintf(buf, buflen, "unavailable");
    }
}

extern "C" int omp_max_threads_available(void) { return omp_get_max_threads(); }

extern "C" const char *normalize_version(void) { return "normalize 1.0.0 (cuda+openmp)"; }

/* Deriva (shift, scale) a partir de los estadísticos y del modo. */
static void affine_from_stats(int mode, const NormStats &s, float *shift, float *scale) {
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

/* ------------------------------- API GPU --------------------------------- */

extern "C" int gpu_normalize(const float *in, float *out, size_t n, int mode,
                             int streams, NormStats *stats, Timing *timing) {
    if (!in || !out || n == 0) return -2;

    const auto t0 = std::chrono::high_resolution_clock::now();

    int devCount = 0;
    CUDA_TRY(cudaGetDeviceCount(&devCount));
    if (devCount == 0) return -3;           /* el llamante debe caer al camino CPU */
    CUDA_TRY(cudaSetDevice(0));

    cudaDeviceProp prop;
    CUDA_TRY(cudaGetDeviceProperties(&prop, 0));

    /* nº de streams: por defecto 4, acotado para no fragmentar los chunks */
    int nStreams = streams > 0 ? streams : 4;
    if ((size_t)nStreams > (n + 1023) / 1024) nStreams = 1;
    omp_set_num_threads(nStreams);

    /* nº de bloques: 32 bloques por SM saturan la latencia sin pasarnos de
       kMaxBlocks parciales que la CPU deba reducir. */
    int nBlocks = prop.multiProcessorCount * 32;
    if (nBlocks > kMaxBlocks) nBlocks = kMaxBlocks;
    if ((size_t)nBlocks * kBlockSize > n) {
        nBlocks = (int)((n + kBlockSize - 1) / kBlockSize);
        if (nBlocks < 1) nBlocks = 1;
    }

    float  *d_in = nullptr, *d_out = nullptr, *d_bMin = nullptr, *d_bMax = nullptr;
    double *d_bSum = nullptr, *d_bSq = nullptr;

    CUDA_TRY(cudaMalloc(&d_in,   n * sizeof(float)));
    CUDA_TRY(cudaMalloc(&d_out,  n * sizeof(float)));
    CUDA_TRY(cudaMalloc(&d_bMin, nBlocks * sizeof(float)));
    CUDA_TRY(cudaMalloc(&d_bMax, nBlocks * sizeof(float)));
    CUDA_TRY(cudaMalloc(&d_bSum, nBlocks * sizeof(double)));
    CUDA_TRY(cudaMalloc(&d_bSq,  nBlocks * sizeof(double)));

    std::vector<cudaStream_t> stream(nStreams);
    for (int s = 0; s < nStreams; ++s) CUDA_TRY(cudaStreamCreate(&stream[s]));

    cudaEvent_t evH2Da, evH2Db, evKa, evKb, evD2Ha, evD2Hb;
    CUDA_TRY(cudaEventCreate(&evH2Da));  CUDA_TRY(cudaEventCreate(&evH2Db));
    CUDA_TRY(cudaEventCreate(&evKa));    CUDA_TRY(cudaEventCreate(&evKb));
    CUDA_TRY(cudaEventCreate(&evD2Ha));  CUDA_TRY(cudaEventCreate(&evD2Hb));

    const size_t chunk = (n + nStreams - 1) / nStreams;

    /* ---- H2D solapado: cada hilo OpenMP empuja su chunk por su stream ---- */
    CUDA_TRY(cudaEventRecord(evH2Da, 0));
    int copyErr = 0;
    #pragma omp parallel for schedule(static) num_threads(nStreams)
    for (int s = 0; s < nStreams; ++s) {
        const size_t off = (size_t)s * chunk;
        if (off >= n) continue;
        const size_t len = std::min(chunk, n - off);
        if (cudaMemcpyAsync(d_in + off, in + off, len * sizeof(float),
                            cudaMemcpyHostToDevice, stream[s]) != cudaSuccess) {
            #pragma omp atomic write
            copyErr = 1;
        }
    }
    for (int s = 0; s < nStreams; ++s) CUDA_TRY(cudaStreamSynchronize(stream[s]));
    if (copyErr) return -4;
    CUDA_TRY(cudaEventRecord(evH2Db, 0));

    /* ---- Pasada 1: estadísticos globales ---- */
    CUDA_TRY(cudaEventRecord(evKa, 0));
    statsKernel<<<nBlocks, kBlockSize, 0, 0>>>(d_in, n, d_bMin, d_bMax, d_bSum, d_bSq);
    CUDA_TRY(cudaGetLastError());
    CUDA_TRY(cudaDeviceSynchronize());

    std::vector<float>  hMin(nBlocks), hMax(nBlocks);
    std::vector<double> hSum(nBlocks), hSq(nBlocks);
    CUDA_TRY(cudaMemcpy(hMin.data(), d_bMin, nBlocks * sizeof(float),  cudaMemcpyDeviceToHost));
    CUDA_TRY(cudaMemcpy(hMax.data(), d_bMax, nBlocks * sizeof(float),  cudaMemcpyDeviceToHost));
    CUDA_TRY(cudaMemcpy(hSum.data(), d_bSum, nBlocks * sizeof(double), cudaMemcpyDeviceToHost));
    CUDA_TRY(cudaMemcpy(hSq.data(),  d_bSq,  nBlocks * sizeof(double), cudaMemcpyDeviceToHost));

    /* Reducción final de nBlocks parciales, paralelizada con OpenMP */
    double gMin = DBL_MAX, gMax = -DBL_MAX, gSum = 0.0, gSq = 0.0;
    #pragma omp parallel for reduction(min:gMin) reduction(max:gMax) \
                             reduction(+:gSum) reduction(+:gSq) schedule(static)
    for (int b = 0; b < nBlocks; ++b) {
        gMin = std::min(gMin, (double)hMin[b]);
        gMax = std::max(gMax, (double)hMax[b]);
        gSum += hSum[b];
        gSq  += hSq[b];
    }

    NormStats st{};
    st.n      = n;
    st.min    = gMin;
    st.max    = gMax;
    st.sum    = gSum;
    st.sumsq  = gSq;
    st.mean   = gSum / (double)n;
    /* varianza poblacional; el max(0,·) protege de negativos por redondeo */
    const double var = std::max(0.0, gSq / (double)n - st.mean * st.mean);
    st.stddev = std::sqrt(var);

    /* ---- Pasada 2: transformación afín ---- */
    float shift = 0.f, scale = 1.f;
    affine_from_stats(mode, st, &shift, &scale);
    applyKernel<<<nBlocks, kBlockSize, 0, 0>>>(d_in, d_out, n, shift, scale);
    CUDA_TRY(cudaGetLastError());
    CUDA_TRY(cudaDeviceSynchronize());
    CUDA_TRY(cudaEventRecord(evKb, 0));

    /* ---- D2H solapado ---- */
    CUDA_TRY(cudaEventRecord(evD2Ha, 0));
    #pragma omp parallel for schedule(static) num_threads(nStreams)
    for (int s = 0; s < nStreams; ++s) {
        const size_t off = (size_t)s * chunk;
        if (off >= n) continue;
        const size_t len = std::min(chunk, n - off);
        if (cudaMemcpyAsync(out + off, d_out + off, len * sizeof(float),
                            cudaMemcpyDeviceToHost, stream[s]) != cudaSuccess) {
            #pragma omp atomic write
            copyErr = 1;
        }
    }
    for (int s = 0; s < nStreams; ++s) CUDA_TRY(cudaStreamSynchronize(stream[s]));
    if (copyErr) return -5;
    CUDA_TRY(cudaEventRecord(evD2Hb, 0));
    CUDA_TRY(cudaEventSynchronize(evD2Hb));

    float msH2D = 0.f, msK = 0.f, msD2H = 0.f;
    CUDA_TRY(cudaEventElapsedTime(&msH2D, evH2Da, evH2Db));
    CUDA_TRY(cudaEventElapsedTime(&msK,   evKa,   evKb));
    CUDA_TRY(cudaEventElapsedTime(&msD2H, evD2Ha, evD2Hb));

    for (int s = 0; s < nStreams; ++s) cudaStreamDestroy(stream[s]);
    cudaEventDestroy(evH2Da); cudaEventDestroy(evH2Db);
    cudaEventDestroy(evKa);   cudaEventDestroy(evKb);
    cudaEventDestroy(evD2Ha); cudaEventDestroy(evD2Hb);
    cudaFree(d_in); cudaFree(d_out);
    cudaFree(d_bMin); cudaFree(d_bMax); cudaFree(d_bSum); cudaFree(d_bSq);

    const auto t1 = std::chrono::high_resolution_clock::now();
    const double totalMs = std::chrono::duration<double, std::milli>(t1 - t0).count();

    if (stats) *stats = st;
    if (timing) {
        timing->h2d_ms    = msH2D;
        timing->kernel_ms = msK;
        timing->d2h_ms    = msD2H;
        timing->total_ms  = totalMs;
        /* 3n lecturas/escrituras en device: leer (p1) + leer (p2) + escribir (p2) */
        timing->gbps = (msK > 0.0) ? (3.0 * n * sizeof(float)) / (msK * 1e6) : 0.0;
    }
    return 0;
}
