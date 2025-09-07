from prometheus_client import Counter, Gauge, Histogram, start_http_server, CollectorRegistry
import threading

class MetricsCollector:
    """
    Colección y exposición de métricas Prometheus para el worker GridMR.
    Cada instancia usa su propio CollectorRegistry para evitar duplicados.
    """
    def __init__(self, port: int = 9100):
        self.registry = CollectorRegistry()
        self.tasks_completed = Counter('worker_tasks_completed', 'Tareas completadas por el worker', registry=self.registry)
        self.tasks_failed = Counter('worker_tasks_failed', 'Tareas fallidas por el worker', registry=self.registry)
        self.tasks_running = Gauge('worker_tasks_running', 'Tareas actualmente en ejecución', registry=self.registry)
        self.cpu_usage = Gauge('worker_cpu_usage', 'Uso de CPU (%)', registry=self.registry)
        self.memory_usage = Gauge('worker_memory_usage', 'Uso de memoria (%)', registry=self.registry)
        self.task_duration = Histogram('worker_task_duration_seconds', 'Duración de tareas en segundos', registry=self.registry)
        self._port = port
        self._server_thread = None

    def start_server(self):
        """Inicia el servidor HTTP de métricas Prometheus en background (una sola vez)."""
        if self._server_thread is None:
            self._server_thread = threading.Thread(target=start_http_server, args=(self._port,), kwargs={"registry": self.registry}, daemon=True)
            self._server_thread.start()

    def increment_counter(self, name: str):
        """Incrementa un contador específico"""
        if name == "tasks_completed":
            self.tasks_completed.inc()
        elif name == "tasks_failed":
            self.tasks_failed.inc()

    def set_gauge(self, name: str, value: float):
        """Establece el valor de una métrica gauge"""
        if name == "active_tasks":
            self.tasks_running.set(value)
        elif name == "queued_tasks":
            # Si tienes una métrica específica para la cola, usa esa. Si no, ignora o crea una Gauge.
            pass
        elif name == "cpu_usage":
            self.cpu_usage.set(value)
        elif name == "memory_usage":
            self.memory_usage.set(value)

    def get_counter(self, name: str) -> int:
        """Obtiene el valor actual de un contador"""
        if name == "tasks_completed":
            return self.tasks_completed._value.get()
        elif name == "tasks_failed":
            return self.tasks_failed._value.get()
        return 0

    def observe_histogram(self, name: str, value: float):
        """Registra una observación en un histograma"""
        if name == "task_duration":
            self.task_duration.observe(value)

# Funciones globales para compatibilidad con código legacy
def start_metrics_server(port: int = 9100):
    """Inicia el servidor de métricas en un puerto específico"""
    start_http_server(port)
