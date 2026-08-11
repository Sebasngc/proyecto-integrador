"""
gpu_normalize.py — Puente ctypes entre Python y libnormalize.so.

Por qué ctypes y no pybind11/CuPy:
  * el .so se compila una sola vez con nvcc y se copia tal cual dentro de la
    imagen de contenedor; no hace falta toolchain de C++ en el entorno de
    ejecución serverless (arranque en frío más corto, imagen más pequeña);
  * evita la dependencia binaria entre la versión de Python y la extensión;
  * numpy expone `ctypes.data_as`, así que no hay copia extra: se pasa el
    puntero al buffer contiguo directamente.

Selección de camino en runtime: se intenta la GPU y, si `gpu_normalize`
devuelve -3 (sin dispositivo) o la librería es la build CPU-only, se cae al
camino OpenMP de forma transparente. El resultado indica cuál se usó.
"""

from __future__ import annotations

import ctypes
import os
import pathlib
from dataclasses import dataclass, asdict
from typing import Literal

import numpy as np

MODES = {"minmax": 0, "zscore": 1, "robust": 2}
ModeName = Literal["minmax", "zscore", "robust"]


class _NormStats(ctypes.Structure):
    _fields_ = [
        ("min", ctypes.c_double),
        ("max", ctypes.c_double),
        ("mean", ctypes.c_double),
        ("stddev", ctypes.c_double),
        ("sum", ctypes.c_double),
        ("sumsq", ctypes.c_double),
        ("n", ctypes.c_size_t),
    ]


class _Timing(ctypes.Structure):
    _fields_ = [
        ("h2d_ms", ctypes.c_double),
        ("kernel_ms", ctypes.c_double),
        ("d2h_ms", ctypes.c_double),
        ("total_ms", ctypes.c_double),
        ("gbps", ctypes.c_double),
    ]


@dataclass(frozen=True)
class Result:
    data: np.ndarray
    backend: str          # "cuda" | "openmp"
    device: str
    stats: dict
    timing: dict

    def to_json(self, include_data: bool = False) -> dict:
        out = {
            "backend": self.backend,
            "device": self.device,
            "stats": self.stats,
            "timing_ms": self.timing,
        }
        if include_data:
            out["data"] = self.data.tolist()
        return out


def _find_library() -> pathlib.Path:
    """Busca libnormalize.so: variable de entorno > junto al módulo > raíz del módulo 01."""
    candidates = []
    if env := os.environ.get("NORMALIZE_LIB"):
        candidates.append(pathlib.Path(env))
    here = pathlib.Path(__file__).resolve().parent
    candidates += [
        here / "libnormalize.so",
        here.parent / "libnormalize.so",
        pathlib.Path("/opt/lib/libnormalize.so"),   # ruta habitual en la imagen Lambda
        pathlib.Path("/usr/local/lib/libnormalize.so"),
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(
        "No se encontró libnormalize.so. Compílala con `make -C 01-gpu-preprocessing` "
        "o define NORMALIZE_LIB con su ruta."
    )


class Normalizer:
    """Carga perezosa y única de la librería nativa (se reutiliza entre invocaciones
    warm de la función serverless: el coste de dlopen se paga una sola vez)."""

    _instance: "Normalizer | None" = None

    def __init__(self) -> None:
        self._lib = ctypes.CDLL(str(_find_library()))
        L = self._lib

        L.gpu_normalize.restype = ctypes.c_int
        L.gpu_normalize.argtypes = [
            ctypes.POINTER(ctypes.c_float), ctypes.POINTER(ctypes.c_float),
            ctypes.c_size_t, ctypes.c_int, ctypes.c_int,
            ctypes.POINTER(_NormStats), ctypes.POINTER(_Timing),
        ]
        L.cpu_normalize_omp.restype = ctypes.c_int
        L.cpu_normalize_omp.argtypes = [
            ctypes.POINTER(ctypes.c_float), ctypes.POINTER(ctypes.c_float),
            ctypes.c_size_t, ctypes.c_int, ctypes.c_int,
            ctypes.POINTER(_NormStats), ctypes.POINTER(_Timing),
        ]
        L.gpu_available.restype = ctypes.c_int
        L.gpu_device_count.restype = ctypes.c_int
        L.gpu_device_name.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_size_t]
        L.omp_max_threads_available.restype = ctypes.c_int
        L.normalize_version.restype = ctypes.c_char_p

    @classmethod
    def get(cls) -> "Normalizer":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ---------------------------------------------------------------- info
    @property
    def has_gpu(self) -> bool:
        return bool(self._lib.gpu_available())

    @property
    def device_name(self) -> str:
        if not self.has_gpu:
            return f"CPU ({self._lib.omp_max_threads_available()} hilos OpenMP)"
        buf = ctypes.create_string_buffer(256)
        self._lib.gpu_device_name(0, buf, 256)
        return buf.value.decode()

    @property
    def version(self) -> str:
        return self._lib.normalize_version().decode()

    def info(self) -> dict:
        return {
            "version": self.version,
            "gpu_available": self.has_gpu,
            "gpu_count": int(self._lib.gpu_device_count()),
            "device": self.device_name,
            "omp_threads": int(self._lib.omp_max_threads_available()),
        }

    # --------------------------------------------------------------- kernel
    def normalize(
        self,
        array: np.ndarray,
        mode: ModeName = "zscore",
        force_cpu: bool = False,
        streams: int = 4,
        threads: int = 0,
    ) -> Result:
        if mode not in MODES:
            raise ValueError(f"modo desconocido: {mode!r}; use uno de {list(MODES)}")

        # float32 contiguo: requisito del kernel. astype ya devuelve una copia
        # contigua, así que no duplicamos memoria innecesariamente.
        src = np.ascontiguousarray(array, dtype=np.float32)
        n = src.size
        if n == 0:
            raise ValueError("el array de entrada está vacío")

        dst = np.empty_like(src)
        stats, timing = _NormStats(), _Timing()
        p_in = src.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
        p_out = dst.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
        m = MODES[mode]

        backend = "openmp"
        rc = -3
        if not force_cpu and self.has_gpu:
            rc = self._lib.gpu_normalize(p_in, p_out, n, m, streams,
                                         ctypes.byref(stats), ctypes.byref(timing))
            if rc == 0:
                backend = "cuda"

        if rc != 0:  # sin GPU, o la GPU falló -> fallback determinista
            rc = self._lib.cpu_normalize_omp(p_in, p_out, n, m, threads,
                                             ctypes.byref(stats), ctypes.byref(timing))
            backend = "openmp"
            if rc != 0:
                raise RuntimeError(f"cpu_normalize_omp falló con código {rc}")

        return Result(
            data=dst.reshape(array.shape) if hasattr(array, "shape") else dst,
            backend=backend,
            device=self.device_name,
            stats={k: v for k, v in asdict_struct(stats).items()},
            timing=asdict_struct(timing),
        )


def asdict_struct(struct: ctypes.Structure) -> dict:
    return {name: getattr(struct, name) for name, _ in struct._fields_}


# ------------------------------------------------------------------ CLI ----
if __name__ == "__main__":
    import argparse, json, time

    ap = argparse.ArgumentParser(description="Normalización CUDA/OpenMP desde Python")
    ap.add_argument("--n", type=int, default=10_000_000)
    ap.add_argument("--mode", default="zscore", choices=list(MODES))
    ap.add_argument("--repeats", type=int, default=5)
    args = ap.parse_args()

    norm = Normalizer.get()
    print(json.dumps(norm.info(), indent=2, ensure_ascii=False))

    rng = np.random.default_rng(42)
    data = rng.normal(100.0, 25.0, args.n).astype(np.float32)

    for label, force_cpu in (("gpu/auto", False), ("cpu/openmp", True)):
        norm.normalize(data[:1000], args.mode, force_cpu=force_cpu)  # warm-up
        times = []
        for _ in range(args.repeats):
            t0 = time.perf_counter()
            res = norm.normalize(data, args.mode, force_cpu=force_cpu)
            times.append((time.perf_counter() - t0) * 1000)
        times.sort()
        print(f"{label:10s} backend={res.backend:7s} mediana={times[len(times)//2]:8.2f} ms "
              f"| kernel={res.timing['kernel_ms']:7.2f} ms | media_out={res.data.mean():+.4f} "
              f"sd_out={res.data.std():.4f}")
