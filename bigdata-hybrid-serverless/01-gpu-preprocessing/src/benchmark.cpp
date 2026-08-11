/* =====================================================================
 *  benchmark.cpp — Arnés de medición GPU vs CPU-OpenMP.
 *
 *  Metodología:
 *   - 1 iteración de calentamiento descartada (inicialización del contexto
 *     CUDA, page faults del alocador, escalado de frecuencia).
 *   - R repeticiones; se reporta la MEDIANA (robusta frente a outliers por
 *     planificación del SO) además de min y media.
 *   - Se valida el resultado GPU contra el CPU (error relativo máximo).
 *
 *  Uso:
 *     ./benchmark --sizes 1e6,1e7,1e8 --repeats 7 --mode zscore --csv out.csv
 * ===================================================================== */

#include "normalize.h"

#include <omp.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <random>
#include <vector>
#include <string>
#include <algorithm>
#include <fstream>

static double median(std::vector<double> v) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    const size_t m = v.size() / 2;
    return (v.size() % 2) ? v[m] : 0.5 * (v[m - 1] + v[m]);
}

static int parse_mode(const std::string &s) {
    if (s == "zscore") return NORM_ZSCORE;
    if (s == "robust") return NORM_ROBUST;
    return NORM_MINMAX;
}

static std::vector<size_t> parse_sizes(const std::string &csv) {
    std::vector<size_t> out;
    size_t start = 0;
    while (start <= csv.size()) {
        const size_t comma = csv.find(',', start);
        const std::string tok = csv.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        if (!tok.empty()) out.push_back((size_t)std::strtod(tok.c_str(), nullptr));
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return out;
}

int main(int argc, char **argv) {
    std::string sizesArg = "1000000,10000000,50000000";
    std::string modeArg  = "zscore";
    std::string csvPath  = "";
    int repeats = 7;
    int streams = 4;

    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--sizes")   && i + 1 < argc) sizesArg = argv[++i];
        else if (!std::strcmp(argv[i], "--repeats") && i + 1 < argc) repeats = std::atoi(argv[++i]);
        else if (!std::strcmp(argv[i], "--mode")    && i + 1 < argc) modeArg = argv[++i];
        else if (!std::strcmp(argv[i], "--streams") && i + 1 < argc) streams = std::atoi(argv[++i]);
        else if (!std::strcmp(argv[i], "--csv")     && i + 1 < argc) csvPath = argv[++i];
        else if (!std::strcmp(argv[i], "--help")) {
            std::printf("uso: %s [--sizes n1,n2] [--repeats R] [--mode minmax|zscore|robust]"
                        " [--streams S] [--csv fichero]\n", argv[0]);
            return 0;
        }
    }

    const int mode = parse_mode(modeArg);
    const auto sizes = parse_sizes(sizesArg);
    const int maxThreads = omp_get_max_threads();

    char devName[256] = "sin GPU";
    if (gpu_available()) gpu_device_name(0, devName, sizeof(devName));

    std::printf("# %s\n", normalize_version());
    std::printf("# GPU        : %s\n", devName);
    std::printf("# CPU hilos  : %d\n", maxThreads);
    std::printf("# modo       : %s | repeticiones: %d | streams: %d\n\n", modeArg.c_str(), repeats, streams);

    std::ofstream csv;
    if (!csvPath.empty()) {
        csv.open(csvPath);
        csv << "n,impl,threads,ms_mediana,ms_min,gbps,h2d_ms,kernel_ms,d2h_ms,speedup_vs_1t,err_rel_max\n";
    }

    std::mt19937 rng(42);
    std::normal_distribution<float> dist(100.0f, 25.0f);

    for (size_t n : sizes) {
        std::vector<float> in(n), outCpu1(n), outCpuN(n), outGpu(n);
        for (size_t i = 0; i < n; ++i) in[i] = dist(rng);

        NormStats s1{}, sN{}, sG{};
        Timing t1{}, tN{}, tG{};
        std::vector<double> msCpu1, msCpuN, msGpu, msGpuKernel;

        /* --- CPU secuencial (baseline del speedup) --- */
        cpu_normalize_omp(in.data(), outCpu1.data(), n, mode, 1, &s1, &t1);   /* warm-up */
        for (int r = 0; r < repeats; ++r) {
            cpu_normalize_omp(in.data(), outCpu1.data(), n, mode, 1, &s1, &t1);
            msCpu1.push_back(t1.total_ms);
        }

        /* --- CPU paralela --- */
        cpu_normalize_omp(in.data(), outCpuN.data(), n, mode, maxThreads, &sN, &tN);
        for (int r = 0; r < repeats; ++r) {
            cpu_normalize_omp(in.data(), outCpuN.data(), n, mode, maxThreads, &sN, &tN);
            msCpuN.push_back(tN.total_ms);
        }

        /* --- GPU --- */
        double errMax = 0.0;
        bool haveGpu = gpu_available() &&
                       gpu_normalize(in.data(), outGpu.data(), n, mode, streams, &sG, &tG) == 0;
        if (haveGpu) {
            for (int r = 0; r < repeats; ++r) {
                gpu_normalize(in.data(), outGpu.data(), n, mode, streams, &sG, &tG);
                msGpu.push_back(tG.total_ms);
                msGpuKernel.push_back(tG.kernel_ms);
            }
            /* validación numérica frente a la referencia CPU */
            #pragma omp parallel for reduction(max:errMax) schedule(static)
            for (long long i = 0; i < (long long)n; ++i) {
                const double ref = outCpu1[i];
                const double got = outGpu[i];
                const double den = std::max(1e-6, std::fabs(ref));
                errMax = std::max(errMax, std::fabs(ref - got) / den);
            }
        }

        const double mc1 = median(msCpu1);
        const double mcN = median(msCpuN);
        const double mg  = haveGpu ? median(msGpu) : 0.0;
        const double mgk = haveGpu ? median(msGpuKernel) : 0.0;

        std::printf("n = %zu (%.1f MB)\n", n, n * sizeof(float) / 1e6);
        std::printf("  CPU  1 hilo   : %9.2f ms   (speedup  1.00x)\n", mc1);
        std::printf("  CPU %2d hilos  : %9.2f ms   (speedup %5.2fx, eficiencia %4.1f%%)\n",
                    maxThreads, mcN, mc1 / mcN, 100.0 * (mc1 / mcN) / maxThreads);
        if (haveGpu) {
            std::printf("  GPU e2e       : %9.2f ms   (speedup %5.2fx vs 1 hilo, %5.2fx vs %d hilos)\n",
                        mg, mc1 / mg, mcN / mg, maxThreads);
            std::printf("  GPU kernel    : %9.2f ms   (speedup %5.2fx vs 1 hilo | BW %6.1f GB/s)\n",
                        mgk, mc1 / mgk, tG.gbps);
            std::printf("  desglose      : H2D %.2f ms | kernel %.2f ms | D2H %.2f ms  -> PCIe = %.0f%% del total\n",
                        tG.h2d_ms, tG.kernel_ms, tG.d2h_ms,
                        100.0 * (tG.h2d_ms + tG.d2h_ms) / std::max(1e-9, tG.total_ms));
            std::printf("  error rel max : %.3e\n", errMax);
        } else {
            std::printf("  GPU           : no disponible (se usa el camino CPU)\n");
        }
        std::printf("\n");

        if (csv.is_open()) {
            csv << n << ",cpu_omp,1,"        << mc1 << "," << *std::min_element(msCpu1.begin(), msCpu1.end())
                << "," << t1.gbps << ",0,"   << mc1 << ",0,1.0,0\n";
            csv << n << ",cpu_omp," << maxThreads << "," << mcN << ","
                << *std::min_element(msCpuN.begin(), msCpuN.end()) << "," << tN.gbps
                << ",0," << mcN << ",0," << (mc1 / mcN) << ",0\n";
            if (haveGpu) {
                csv << n << ",gpu_cuda," << streams << "," << mg << ","
                    << *std::min_element(msGpu.begin(), msGpu.end()) << "," << tG.gbps << ","
                    << tG.h2d_ms << "," << tG.kernel_ms << "," << tG.d2h_ms << ","
                    << (mc1 / mg) << "," << errMax << "\n";
            }
        }
    }

    if (csv.is_open()) { csv.close(); std::printf("CSV escrito en %s\n", csvPath.c_str()); }
    return 0;
}
