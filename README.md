# ST0263 - Tópicos Especiales en Telemática

# Estudiantes:
- Samuel Andrés Ariza Gómez
- Juan Manuel Montoya Arango 
- Isabela Osorio

# Profesor: 
Edwin Nelson Montoya Munera, emontoya@eafit.edu.co

# GridMR - Framework Distribuido de MapReduce

# 1. Breve descripción de la actividad

GridMR es un framework distribuido de MapReduce implementado en Python para procesamiento escalable de big data a través de múltiples nodos worker. El framework proporciona una implementación completa del paradigma MapReduce con orquestación automática de trabajos, tolerancia a fallos, aislamiento de seguridad y capacidades profesionales de computación distribuida.

La solución implementa una arquitectura master-worker con almacenamiento compartido basado en NFS, permitiendo el procesamiento distribuido de grandes volúmenes de datos mediante la división en tareas de mapeo y reducción que se ejecutan de manera paralela en contenedores Docker.

## 1.1. Aspectos cumplidos de la actividad propuesta

### Requerimientos Funcionales:
- ✅ **Implementación completa de MapReduce**: Flujo automático Map→Reduce con orquestación inteligente de tareas
- ✅ **Procesamiento distribuido**: Escalamiento horizontal a través de múltiples nodos worker con balanceamento de carga
- ✅ **Sistema de archivos compartido**: NFS distribuido para acceso transparente a datos
- ✅ **API REST completa**: Endpoints para envío de trabajos, monitoreo y recuperación de resultados
- ✅ **Ejecución segura de código**: Ambiente sandboxed para ejecución segura de funciones definidas por el usuario
- ✅ **Monitoreo y logging**: Sistema comprensivo de monitoreo con métricas detalladas de ejecución
- ✅ **Containerización**: Despliegue orquestado con Docker Compose y networking apropiado
- ✅ **Testing automatizado**: Suite completa de pruebas de rendimiento y generación de datos de prueba

### Requerimientos No Funcionales:
- ✅ **Tolerancia a fallos**: Mecanismos automáticos de retry y manejo graceful de errores
- ✅ **Escalabilidad**: Arquitectura diseñada para escalamiento horizontal
- ✅ **Seguridad**: Aislamiento de procesos y validación de código
- ✅ **Mantenibilidad**: Código de calidad empresarial con documentación profesional
- ✅ **Performance**: Optimizaciones para procesamiento de grandes volúmenes de datos

## 1.2. Aspectos NO cumplidos de la actividad propuesta

### Limitaciones identificadas:
- ⚠️ **Interface gráfica**: No se implementó una interfaz web para administración
- ⚠️ **Persistencia avanzada**: No se implementó base de datos para historial de trabajos
- ⚠️ **Autenticación**: Sistema de autenticación y autorización básico
- ⚠️ **Métricas avanzadas**: Dashboard de métricas en tiempo real no implementado
- ⚠️ **Recuperación automática**: Checkpoint y recovery automático de trabajos en caso de fallo del master

# 2. Información general de diseño de alto nivel, arquitectura, patrones y mejores prácticas

## 2.1. Arquitectura del Sistema

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Client API    │    │   Master Node    │    │  Worker Nodes   │
│                 │    │                  │    │                 │
│ Job Submission  │───▶│  Job Manager     │───▶│  Task Executor  │
│ Status Monitor  │    │  Task Scheduler  │    │  Map Processor  │
│ Result Access   │    │  Worker Registry │    │  Reduce Process │
└─────────────────┘    │  Resource Mgmt   │    │  Sandbox Engine │
                       └──────────────────┘    └─────────────────┘
                                │                        │
                                └────────────────────────┘
                                    Distributed NFS Storage
```

## 2.2. Patrones de Diseño Implementados

### Patrones Arquitectónicos:
- **Master-Worker Pattern**: Nodo master coordina múltiples workers para procesamiento distribuido
- **Producer-Consumer Pattern**: Queue de tareas entre master y workers
- **Shared Storage Pattern**: NFS como almacenamiento compartido para datos distribuidos

### Patrones de Código:
- **Dependency Injection**: Gestión de dependencias en servicios
- **Factory Pattern**: Creación de ejecutores de tareas especializados
- **Observer Pattern**: Sistema de eventos para monitoreo de trabajos
- **Strategy Pattern**: Diferentes estrategias de mapeo y reducción

## 2.3. Mejores Prácticas Implementadas

### Containerización:
- Multi-stage Docker builds para optimización de imágenes
- Non-root users para seguridad
- Health checks automáticos
- Volume management para persistencia

### Seguridad:
- Sandboxed execution environment
- User isolation en contenedores
- Network segmentation
- Input validation y sanitization

### Observabilidad:
- Structured logging con correlation IDs
- Health endpoints para monitoring
- Metrics collection y exposición
- Distributed tracing capabilities

# 3. Descripción del ambiente de desarrollo y técnico

## 3.1. Stack Tecnológico

### Lenguajes y Frameworks:
- **Python 3.10**: Lenguaje principal del framework
- **FastAPI 0.104.1**: Framework web para APIs REST
- **Uvicorn 0.24.0**: Servidor ASGI de alto rendimiento
- **Docker 24.0+**: Containerización y orquestación
- **Docker Compose 2.0+**: Orquestación multi-contenedor

### Librerías Principales:
```
fastapi==0.104.1
uvicorn[standard]==0.24.0
pydantic==2.5.0
httpx==0.25.2
aiofiles==23.2.1
psutil==5.9.6
```

### Dependencias del Sistema:
- **NFS Utils**: Sistema de archivos distribuido
- **curl**: Health checks y debugging
- **htop**: Monitoreo de recursos

## 3.2. Compilación y Ejecución

### Preparación del Ambiente:
```bash
# Clonar repositorio
git clone <repository-url>
cd gridMR

# Configurar permisos de datos
sudo chown -R $USER:$USER data/
chmod 755 data/

# Verificar instalación de Docker
docker --version
docker-compose --version
```

### Construcción y Despliegue:
```bash
# Construir todas las imágenes
docker-compose build

# Iniciar servicios distribuidos
docker-compose up -d

# Verificar estado de servicios
docker-compose ps
```

## 3.3. Detalles del Desarrollo

### Arquitectura de Componentes:

#### Master Node ([master/](master/)):
- **Job Manager**: Gestión del ciclo de vida de trabajos
- **Task Scheduler**: Distribución inteligente de tareas
- **Worker Registry**: Registro y monitoreo de workers
- **Resource Manager**: Gestión de recursos y balanceamiento

#### Worker Nodes ([worker/](worker/)):
- **Task Executor**: Motor de ejecución de tareas
- **Sandbox Engine**: Aislamiento seguro de código usuario
- **Map/Reduce Processors**: Procesadores especializados
- **Health Monitor**: Monitoreo de salud del worker

#### NFS Infrastructure:
- **NFS Server** ([nfs-server/](nfs-server/)): Servidor de archivos distribuido
- **NFS Client** ([nfs-client/](nfs-client/)): Cliente para acceso compartido

## 3.4. Configuración de Parámetros

### Variables de Ambiente:
```yaml
# Master Configuration
GRIDMR_MASTER_HOST: "0.0.0.0"
GRIDMR_MASTER_PORT: 8000
GRIDMR_LOG_LEVEL: "INFO"

# Worker Configuration  
GRIDMR_WORKER_WORKER_PORT: 8001
GRIDMR_WORKER_CPU_CORES: 2
GRIDMR_WORKER_MEMORY_MB: 1024

# NFS Configuration
NFS_EXPORT_PATH: "/exports"
NFS_CLIENT_MOUNT: "/data"
```

### Configuración de Red:
```yaml
# docker-compose.yml networking
networks:
  gridmr-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

### Configuración de Volúmenes:
```yaml
volumes:
  - ./data:/data:rw           # Datos compartidos
  - ./logs:/app/logs:rw       # Logs de aplicación
  - /var/run/docker.sock:/var/run/docker.sock  # Docker API
```

## 3.5. Estructura de Directorios

```
gridMR/
├── master/                          # Nodo maestro
│   ├── Dockerfile                   # Imagen del master
│   ├── main.py                      # Punto de entrada FastAPI
│   ├── requirements.txt             # Dependencias Python
│   ├── job_manager.py               # Gestión de trabajos
│   ├── worker_registry.py           # Registro de workers
│   └── task_scheduler.py            # Planificación de tareas
│
├── worker/                          # Nodos worker
│   ├── Dockerfile                   # Imagen del worker
│   ├── main.py                      # Servidor worker FastAPI
│   ├── task_executor.py             # Ejecutor de tareas
│   ├── sandbox.py                   # Entorno de ejecución seguro
│   └── processors/                  # Procesadores especializados
│       ├── map_processor.py         # Procesador de mapeo
│       └── reduce_processor.py      # Procesador de reducción
│
├── data/                            # Datos compartidos
│   ├── input.txt                    # Archivo de entrada
│   ├── input_split_*                # Fragmentos de entrada
│   └── output/                      # Resultados de procesamiento
│
├── nfs-server/                      # Servidor NFS
│   ├── Dockerfile                   # Imagen NFS server
│   └── exports                      # Configuración de exports
│
├── nfs-client/                      # Cliente NFS
│   └── Dockerfile                   # Imagen NFS client
│
├── docker-compose.yml               # Orquestación de contenedores
├── payload.json                     # Ejemplo de trabajo MapReduce
├── automated_performance_test.py    # Suite de pruebas de rendimiento
├── generate_test_file.py            # Generador de datos de prueba
└── README.md                        # Documentación del proyecto
```

# 4. Descripción del ambiente de EJECUCIÓN (producción)

## 4.1. Requerimientos del Sistema

### Hardware Mínimo:
- **CPU**: 4 cores (recomendado 8+ cores)
- **RAM**: 4GB (recomendado 8GB+)
- **Storage**: 20GB espacio libre
- **Network**: 1Gbps para clusters distribuidos

### Software Base:
- **OS**: Ubuntu 20.04+ / CentOS 8+ / macOS 11+
- **Docker Engine**: 24.0+
- **Docker Compose**: 2.0+
- **Python**: 3.8+ (para herramientas cliente)

## 4.2. Configuración de Producción

### Direcciones IP y Puertos:
```
Servicio              Puerto    Protocolo    Descripción
Master Node          8000      HTTP/REST    API principal
Worker Node 1        8001      HTTP/REST    Worker MapReduce
Worker Node 2        8002      HTTP/REST    Worker Reduce
NFS Server           2049      NFS/TCP      Sistema archivos
RPC Portmapper       111       RPC/TCP      Mapeo de puertos NFS
```

### Variables de Ambiente de Producción:
```bash
# Configuración optimizada para producción
export GRIDMR_ENVIRONMENT=production
export GRIDMR_LOG_LEVEL=WARNING
export GRIDMR_MAX_WORKERS=10
export GRIDMR_TASK_TIMEOUT=3600
export GRIDMR_MEMORY_LIMIT=2048
```

## 4.3. Lanzamiento del Servidor

### Inicio de Servicios:
```bash
# Preparación del ambiente
sudo usermod -aG docker $USER
sudo systemctl start docker
sudo systemctl enable docker

# Lanzamiento del cluster
cd gridMR
docker-compose -f docker-compose.yml up -d

# Verificación de estado
docker-compose ps
curl http://localhost:8000/health
```

### Monitoreo de Servicios:
```bash
# Logs en tiempo real
docker-compose logs -f master
docker-compose logs -f worker1

# Métricas del sistema
curl http://localhost:8000/api/v1/metrics/system
curl http://localhost:8000/api/v1/workers/status
```

## 4.4. Guía de Usuario

### 4.4.1. Envío de Trabajo MapReduce

```bash
# Ejemplo: Conteo de palabras
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "word_count_job",
    "input_file": "/data/input.txt",
    "map_function": "def map_func(line): return [(word, 1) for word in line.strip().split()]",
    "reduce_function": "def reduce_func(word, counts): return (word, sum(counts))",
    "num_reducers": 2
  }'
```

### 4.4.2. Monitoreo de Progreso

```bash
# Obtener estado del trabajo
JOB_ID="550e8400-e29b-41d4-a716-446655440000"
curl http://localhost:8000/api/v1/jobs/${JOB_ID}/status

# Métricas detalladas
curl http://localhost:8000/api/v1/jobs/${JOB_ID}/metrics
```

### 4.4.3. Recuperación de Resultados

```bash
# Descargar resultados
curl http://localhost:8000/api/v1/jobs/${JOB_ID}/results > results.txt

# Verificar archivos de salida
ls -la data/output/
```

### 4.4.4. Ejemplo Completo de WordCount

```python
# Preparar datos de entrada
echo "hello world hello python world" > data/input.txt

# Enviar trabajo
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d @payload.json

# Monitorear hasta completar
while true; do
  STATUS=$(curl -s http://localhost:8000/api/v1/jobs/${JOB_ID}/status | jq -r '.status')
  echo "Status: $STATUS"
  [[ "$STATUS" == "completed" ]] && break
  sleep 5
done

# Obtener resultados
curl http://localhost:8000/api/v1/jobs/${JOB_ID}/results
```

## 4.5. Pruebas de Rendimiento

### Suite Automatizada:
```bash
# Ejecutar pruebas de rendimiento
python automated_performance_test.py

# Generar datos de prueba grandes
python generate_test_file.py --size 100MB --output data/large_input.txt

# Benchmark personalizado
curl -X POST http://localhost:8000/api/v1/benchmark \
  -d '{"data_size": "1GB", "workers": 4, "iterations": 10}'
```

# 5. Información adicional relevante

## 5.1. Características Avanzadas

### Tolerancia a Fallos:
- Retry automático de tareas fallidas
- Detección de workers inactivos
- Redistribución automática de carga
- Graceful shutdown con cleanup

### Optimizaciones de Rendimiento:
- Streaming de datos para archivos grandes
- Pool de conexiones HTTP reutilizables
- Caching inteligente de resultados intermedios
- Balanceamento dinámico de carga

### Seguridad:
- Ejecución sandboxed de código usuario
- Validación de sintaxis antes de ejecución
- Limits de recursos por tarea
- Network isolation entre componentes

## 5.2. Extensibilidad

El framework está diseñado para extensión mediante:
- Plugins de procesadores personalizados
- Estrategias de particionamiento configurable
- Backends de almacenamiento intercambiables
- Métricas y monitoring customizable

## 5.3. Casos de Uso Recomendados

- **Análisis de logs**: Procesamiento de grandes volúmenes de logs
- **ETL distribuido**: Transformación de datos masivos
- **Machine Learning**: Entrenamiento distribuido de modelos
- **Análisis de texto**: NLP y minería de texto
- **Procesamiento de imágenes**: Análisis masivo de contenido multimedia

# Referencias

## Documentación Técnica:
- **MapReduce Paper**: Dean, J., & Ghemawat, S. (2008). MapReduce: Simplified Data Processing on Large Clusters
- **FastAPI Documentation**: https://fastapi.tiangolo.com/
- **Docker Best Practices**: https://docs.docker.com/develop/dev-best-practices/

## Recursos de Implementación:
- **Python AsyncIO**: https://docs.python.org/3/library/asyncio.html
- **NFS Configuration**: https://ubuntu.com/server/docs/service-nfs
- **Container Security**: https://kubernetes.io/docs/concepts/security/

## Inspiración de Arquitectura:
- **Apache Hadoop MapReduce**: https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/
- **Google MapReduce**: Research paper implementation patterns