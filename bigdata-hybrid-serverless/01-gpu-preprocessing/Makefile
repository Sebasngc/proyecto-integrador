# ============================================================
#  Módulo 1 — Preprocesamiento GPU (CUDA + OpenMP)
#
#  make            -> libnormalize.so + benchmark (GPU si hay nvcc)
#  make cpu-only   -> compila sólo el camino OpenMP (sin CUDA)
#  make bench      -> ejecuta el benchmark y guarda ../results/gpu_bench.csv
#  make test       -> valida GPU vs CPU sobre tamaños pequeños
# ============================================================

NVCC      := $(shell command -v nvcc 2>/dev/null)
CXX       ?= g++
ARCH      ?= sm_75          # T4=sm_75, A10G=sm_86, L4=sm_89, A100=sm_80, H100=sm_90
INCLUDES  := -Iinclude
CXXFLAGS  := -O3 -march=native -fopenmp -fPIC -std=c++17 $(INCLUDES)
NVCCFLAGS := -O3 -std=c++17 -arch=$(ARCH) -Xcompiler "-fopenmp -fPIC -O3" $(INCLUDES)
LDLIBS    := -lgomp -lm

SRC_CU    := src/normalize.cu
SRC_CPU   := src/normalize_cpu.cpp
SRC_BENCH := src/benchmark.cpp
LIB       := libnormalize.so
BIN       := benchmark

ifdef NVCC
all: $(LIB) $(BIN)
	@echo ">> compilado CON soporte CUDA (arch=$(ARCH))"
else
all: cpu-only
	@echo ">> nvcc no encontrado: compilado SÓLO con el camino CPU/OpenMP"
endif

$(LIB): $(SRC_CU) $(SRC_CPU) include/normalize.h
	$(NVCC) $(NVCCFLAGS) --shared $(SRC_CU) $(SRC_CPU) -o $@ $(LDLIBS)

$(BIN): $(SRC_BENCH) $(LIB)
	$(NVCC) $(NVCCFLAGS) $(SRC_BENCH) -o $@ -L. -lnormalize -Wl,-rpath,'$$ORIGIN' $(LDLIBS)

# ---- Camino sin GPU: stubs para que la librería cargue igual en Lambda ----
cpu-only: libnormalize_cpu.so benchmark_cpu
	@cp -f libnormalize_cpu.so $(LIB) 2>/dev/null || true
	@cp -f benchmark_cpu $(BIN) 2>/dev/null || true

libnormalize_cpu.so: $(SRC_CPU) src/cuda_stubs.cpp include/normalize.h
	$(CXX) $(CXXFLAGS) --shared $(SRC_CPU) src/cuda_stubs.cpp -o $@ $(LDLIBS)

benchmark_cpu: $(SRC_BENCH) libnormalize_cpu.so
	$(CXX) $(CXXFLAGS) $(SRC_BENCH) -o $@ -L. -lnormalize_cpu -Wl,-rpath,'$$ORIGIN' $(LDLIBS)

bench: all
	@mkdir -p ../results
	./$(BIN) --sizes 1000000,10000000,50000000,100000000 --repeats 7 \
	         --mode zscore --streams 4 --csv ../results/gpu_bench.csv

test: all
	./$(BIN) --sizes 100000,1000000 --repeats 3 --mode minmax

clean:
	rm -f $(LIB) libnormalize_cpu.so $(BIN) benchmark_cpu *.o

.PHONY: all cpu-only bench test clean
