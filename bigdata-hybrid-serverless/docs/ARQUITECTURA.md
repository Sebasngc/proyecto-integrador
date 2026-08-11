# Diagramas de arquitectura

Complementa `informe/INFORME.md §2`. Los diagramas están en Mermaid: se renderizan
directamente en GitHub y en la mayoría de editores Markdown.

## 1. Secuencia de una petición completa

```mermaid
sequenceDiagram
    autonumber
    participant C as Cliente
    participant API as Akka HTTP
    participant REG as JobRegistry
    participant ORCH as JobOrchestrator
    participant V as ValidationActor
    participant G as GpuPreprocessActor
    participant GPU as Servicio GPU
    participant S as SparkJobActor
    participant EMR as EMR Serverless
    participant A as ResultAnalyzer
    participant R as ResponderActor

    C->>API: POST /api/v1/jobs
    API->>REG: SubmitJob
    REG->>ORCH: spawn + Start
    ORCH-->>API: Ack(jobId)
    API-->>C: 202 Accepted + jobId

    ORCH->>V: Validate
    V-->>ORCH: Valid(normalizado)

    ORCH->>G: Preprocess
    G->>GPU: POST /preprocess
    GPU-->>G: {backend, stats, output_uri}
    G-->>ORCH: Done(GpuResult)

    ORCH->>S: Submit
    par pipelines en paralelo
        S->>EMR: StartJobRun (rdd)
    and
        S->>EMR: StartJobRun (dataframe)
    end
    loop sondeo cada 10 s
        S->>EMR: GetJobRun
    end
    S-->>ORCH: Done([rdd, dataframe])

    ORCH->>A: Analyze
    A-->>ORCH: Analysis(speedup, avisos)

    ORCH->>R: Deliver
    R->>R: persistir en DynamoDB
    R-->>C: webhook (opcional)
    R-->>ORCH: Delivered

    C->>API: GET /jobs/{id}/result
    API-->>C: 200 + análisis
```

## 2. Máquina de estados del job

```mermaid
stateDiagram-v2
    [*] --> Received: POST /jobs
    Received --> Validating: JobAccepted
    Validating --> GpuPhase: InputValidated
    Validating --> Failed: entrada inválida (sin reintento)
    GpuPhase --> SparkPhase: GpuCompleted
    GpuPhase --> GpuPhase: reintento con backoff
    GpuPhase --> Failed: maxAttempts agotados
    SparkPhase --> Analyzing: SparkCompleted
    SparkPhase --> SparkPhase: reintento (máx. 2)
    SparkPhase --> Failed: maxAttempts agotados
    Analyzing --> Responding: AnalysisCompleted
    Responding --> Completed: ResponseDelivered
    Completed --> [*]
    Failed --> [*]
```

## 3. Camino de los datos

```mermaid
flowchart LR
    RAW[("Dataset crudo<br/>Parquet · CSV · npy")] --> GPU

    subgraph GPU["Fase GPU"]
        direction TB
        H2D[H2D<br/>PCIe] --> K1[Pasada 1<br/>min max Σx Σx²]
        K1 --> RED[Reducción en CPU<br/>OpenMP]
        RED --> K2[Pasada 2<br/>transformación afín]
        K2 --> D2H[D2H<br/>PCIe]
    end

    GPU --> NORM[("Dataset normalizado")]
    NORM --> RDDP[Pipeline RDD]
    NORM --> DFP[Pipeline DataFrame]
    RDDP --> RES[("Agregados + métricas")]
    DFP --> RES
    RES --> ANL[Análisis:<br/>speedup y anomalías]
```

## 4. Dónde se ejecuta cada cosa

```mermaid
flowchart TB
    subgraph SL["Serverless (escala a cero)"]
        F1[Servicio GPU<br/>Cloud Run + L4]
        F2[Lanzador Spark<br/>AWS Lambda]
        F3[EMR Serverless]
    end
    subgraph LV["Proceso de larga vida (con estado)"]
        O[Orquestador Akka<br/>Fargate · Cloud Run mín. 1]
    end
    subgraph ST["Almacenamiento gestionado"]
        S3[(S3 · GCS)]
        DDB[(DynamoDB)]
        JRN[(Journal de eventos)]
    end
    O --> F1 & F2
    F2 --> F3
    F1 & F3 --> S3
    O --> DDB & JRN
```
