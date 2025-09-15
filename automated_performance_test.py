#!/usr/bin/env python3
"""
GridMR Automated Performance Testing Suite
==========================================
This script automates comprehensive performance testing of the GridMR system:
- Generates test files of various sizes
- Submits MapReduce jobs via API
- Monitors job execution and collects metrics
- Generates professional performance reports
"""

import os
import sys
import json
import time
import requests
import psutil
import threading
import subprocess
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import csv

class PerformanceMonitor:
    """Monitors system performance metrics during job execution."""
    
    def __init__(self):
        self.monitoring = False
        self.metrics = []
        self.monitor_thread = None
        
    def start_monitoring(self, interval=2):
        """Start performance monitoring in background thread."""
        self.monitoring = True
        self.metrics = []
        self.monitor_thread = threading.Thread(target=self._monitor_loop, args=(interval,))
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
    def stop_monitoring(self):
        """Stop performance monitoring and return collected metrics."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        return self.metrics
        
    def _monitor_loop(self, interval):
        """Background monitoring loop."""
        while self.monitoring:
            try:
                # System metrics
                cpu_percent = psutil.cpu_percent(interval=0.1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                # Docker container metrics (if available)
                docker_stats = self._get_docker_stats()
                
                metric = {
                    'timestamp': datetime.now().isoformat(),
                    'cpu_percent': cpu_percent,
                    'memory_used_mb': memory.used / (1024 * 1024),
                    'memory_percent': memory.percent,
                    'disk_used_gb': disk.used / (1024 * 1024 * 1024),
                    'docker_stats': docker_stats
                }
                
                self.metrics.append(metric)
                time.sleep(interval)
                
            except Exception as e:
                print(f"Error in monitoring: {e}")
                time.sleep(interval)
                
    def _get_docker_stats(self):
        """Get Docker container statistics."""
        try:
            result = subprocess.run(
                ['docker', 'stats', '--no-stream', '--format', 'json'],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                stats = []
                for line in lines:
                    if line:
                        stats.append(json.loads(line))
                return stats
        except Exception:
            pass
        return []

class TestFileGenerator:
    """Generates test files of specific sizes for performance testing."""
    
    SAMPLE_WORDS = [
        "mapreduce", "distributed", "computing", "performance", "analysis", "data",
        "processing", "parallel", "algorithm", "cluster", "node", "worker", "master",
        "task", "job", "scheduler", "partition", "reduce", "map", "function",
        "hadoop", "spark", "framework", "bigdata", "analytics", "scalable",
        "fault", "tolerant", "throughput", "latency", "optimization", "efficiency"
    ]
    
    SAMPLE_SENTENCES = [
        "MapReduce is a programming model for processing large datasets in parallel.",
        "The distributed computing framework enables scalable data processing across clusters.",
        "Performance analysis shows significant improvements in processing throughput.",
        "Worker nodes execute map and reduce tasks efficiently across the cluster.",
        "The master node coordinates job scheduling and task distribution.",
        "Fault tolerance mechanisms ensure system reliability during failures.",
        "Data partitioning strategies optimize load balancing across workers.",
        "Monitoring metrics provide insights into system performance characteristics."
    ]
    
    def generate_file(self, size_mb: float, output_path: str) -> str:
        """Generate a test file with realistic content."""
        import random
        
        target_bytes = int(size_mb * 1024 * 1024)
        content = []
        current_size = 0
        line_number = 1
        
        print(f"📄 Generating {size_mb}MB test file...")
        
        while current_size < target_bytes:
            if random.random() < 0.7:
                sentence = random.choice(self.SAMPLE_SENTENCES)
                if random.random() < 0.3:
                    words = sentence.split()
                    if len(words) > 3:
                        replace_idx = random.randint(1, len(words) - 2)
                        words[replace_idx] = random.choice(self.SAMPLE_WORDS)
                        sentence = " ".join(words)
                line = f"Line {line_number:06d}: {sentence}"
            else:
                num_words = random.randint(5, 15)
                words = random.sample(self.SAMPLE_WORDS, min(num_words, len(self.SAMPLE_WORDS)))
                line = f"Data {line_number:06d}: {' '.join(words)}"
            
            if random.random() < 0.1:
                line += f" [timestamp: {int(time.time() * 1000000)}]"
            
            line += "\n"
            content.append(line)
            current_size += len(line.encode('utf-8'))
            line_number += 1
            
            if line_number % 5000 == 0:
                progress = (current_size / target_bytes) * 100
                print(f"    Progress: {progress:.1f}%")
        
        # Write file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("".join(content))
        
        actual_size = os.path.getsize(output_path) / (1024 * 1024)
        print(f"✅ File generated: {actual_size:.2f}MB at {output_path}")
        
        return output_path

class GridMRAPIClient:
    """Client for interacting with GridMR API."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        
    def health_check(self) -> bool:
        """Check if the API is healthy."""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/health", timeout=5)
            if response.status_code == 200:
                print(f"✅ API health check passed")
                return True
        except Exception:
            pass
                
        return False
            
    def submit_job(self, payload: Dict) -> Optional[str]:
        """Submit a MapReduce job and return job ID."""
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/jobs",
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            job_id = data.get('job_id')
            if job_id:
                print(f"✅ Job submitted successfully: {job_id[:8]}...")
                return job_id
        except Exception as e:
            print(f"❌ Error submitting job: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"❌ Response content: {e.response.text}")
                
        return None
            
    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get job status and details."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v1/jobs/{job_id}",
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Error getting job status: {e}")
            
        return None
            
    def wait_for_job_completion(self, job_id: str, timeout: int = 300) -> Dict:
        """Wait for job completion and return final status."""
        start_time = time.time()
        last_status = "unknown"
        
        while time.time() - start_time < timeout:
            status_data = self.get_job_status(job_id)
            if not status_data:
                time.sleep(5)
                continue
                
            current_status = status_data.get('status', 'unknown')
            
            if current_status != last_status:
                print(f"📊 Job {job_id[:8]}... status: {current_status}")
                last_status = current_status
            
            if current_status in ['completed', 'failed', 'error']:
                return status_data
                
            time.sleep(5)
            
        print(f"⏰ Job {job_id[:8]}... timed out after {timeout}s")
        return {'status': 'timeout', 'job_id': job_id}

class PerformanceTestSuite:
    """Main performance testing suite."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.api_client = GridMRAPIClient(base_url)
        self.file_generator = TestFileGenerator()
        self.monitor = PerformanceMonitor()
        self.results = []
        
    def run_test_suite(self, file_sizes_mb: List[float], iterations: int = 1):
        """Run complete performance test suite."""
        print("🚀 Starting GridMR Performance Test Suite")
        print(f"📏 File sizes: {file_sizes_mb} MB")
        print(f"🔄 Iterations per size: {iterations}")
        print("=" * 60)
        
        # Check API health
        if not self.api_client.health_check():
            print("❌ API health check failed. Make sure GridMR is running.")
            return
            
        print("✅ API health check passed")
        
        for size_mb in file_sizes_mb:
            for iteration in range(iterations):
                print(f"\n🧪 Testing {size_mb}MB file (iteration {iteration + 1}/{iterations})")
                self._run_single_test(size_mb, iteration)
                
        self._generate_reports()
        
    def _run_single_test(self, size_mb: float, iteration: int):
        """Run a single performance test."""
        test_id = f"{size_mb}MB_iter{iteration + 1}"
        
        # Generate test file in the Docker-mounted data directory
        input_file = "data/input.txt"
        self.file_generator.generate_file(size_mb, input_file)
        
        # Verify file was created successfully
        if not os.path.exists(input_file):
            print(f"❌ Failed to create input file: {input_file}")
            return
            
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)
        
        # Create payload with absolute paths (as expected by the GridMR API)
        payload = {
            "client_id": f"perf-test-{test_id}",
            "job_name": f"wordcount_{test_id}",
            "map_function": "def map_fn(line):\n    for word in line.strip().split():\n        yield (word.lower(), 1)",
            "reduce_function": "def reduce_fn(key, values):\n    return (key, sum(values))",
            "input_data_path": "/data/input.txt",  # Use absolute path for Docker
            "output_data_path": f"/data/output_wordcount_{test_id}.txt",  # Use absolute path for Docker
            "split_size": max(1, int(size_mb / 4)),  # Dynamic split size
            "num_reducers": min(3, max(1, int(size_mb / 2))),  # Dynamic reducers
            "parameters": {}
        }
        
        # Start monitoring
        print("📈 Starting performance monitoring...")
        self.monitor.start_monitoring(interval=2)
        
        # Submit job
        start_time = time.time()
        print(f"📤 Submitting job...")
        job_id = self.api_client.submit_job(payload)
        
        if not job_id:
            print(f"❌ Failed to submit job for {test_id}")
            self.monitor.stop_monitoring()
            return
            
        submit_time = time.time()
        print(f"✅ Job submitted: {job_id[:8]}...")
        
        # Wait for completion
        print(f"⏳ Waiting for job completion...")
        final_status = self.api_client.wait_for_job_completion(job_id)
        completion_time = time.time()
        
        # Stop monitoring
        metrics = self.monitor.stop_monitoring()
        
        # Calculate execution time
        total_time = completion_time - start_time
        execution_time = completion_time - submit_time
        
        # Collect results
        result = {
            'test_id': test_id,
            'file_size_mb': size_mb,
            'iteration': iteration + 1,
            'job_id': job_id,
            'start_time': datetime.fromtimestamp(start_time).isoformat(),
            'submit_time': datetime.fromtimestamp(submit_time).isoformat(),
            'completion_time': datetime.fromtimestamp(completion_time).isoformat(),
            'total_time_seconds': total_time,
            'execution_time_seconds': execution_time,
            'job_status': final_status.get('status'),
            'total_tasks': final_status.get('total_tasks', 0),
            'completed_tasks': final_status.get('completed_tasks', 0),
            'failed_tasks': final_status.get('failed_tasks', 0),
            'split_size': payload['split_size'],
            'num_reducers': payload['num_reducers'],
            'performance_metrics': metrics,
            'avg_cpu_percent': sum(m['cpu_percent'] for m in metrics) / len(metrics) if metrics else 0,
            'max_memory_mb': max(m['memory_used_mb'] for m in metrics) if metrics else 0,
            'avg_memory_mb': sum(m['memory_used_mb'] for m in metrics) / len(metrics) if metrics else 0
        }
        
        self.results.append(result)
        
        # Print summary
        status_emoji = "✅" if final_status.get('status') == 'completed' else "❌"
        print(f"{status_emoji} Test {test_id} completed:")
        print(f"    Total time: {total_time:.2f}s")
        print(f"    Execution time: {execution_time:.2f}s")
        print(f"    Status: {final_status.get('status')}")
        print(f"    Tasks: {final_status.get('completed_tasks', 0)}/{final_status.get('total_tasks', 0)}")
        print(f"    Avg CPU: {result['avg_cpu_percent']:.1f}%")
        print(f"    Max Memory: {result['max_memory_mb']:.1f}MB")
        
    def _generate_reports(self):
        """Generate comprehensive performance reports."""
        print("\n" + "=" * 60)
        print("📊 GENERATING PERFORMANCE REPORTS")
        print("=" * 60)
        
        # Create reports directory
        reports_dir = Path("performance_reports")
        reports_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate CSV report
        csv_file = reports_dir / f"performance_results_{timestamp}.csv"
        self._generate_csv_report(csv_file)
        
        # Generate JSON report
        json_file = reports_dir / f"performance_results_{timestamp}.json"
        self._generate_json_report(json_file)
        
        # Generate HTML report
        html_file = reports_dir / f"performance_report_{timestamp}.html"
        self._generate_html_report(html_file)
        
        # Print summary
        self._print_summary()
        
        print(f"\n📁 Reports generated:")
        print(f"    CSV: {csv_file}")
        print(f"    JSON: {json_file}")
        print(f"    HTML: {html_file}")
        print(f"\n🌐 Open HTML report: file://{html_file.absolute()}")
        
    def _generate_csv_report(self, csv_file: Path):
        """Generate CSV performance report."""
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'test_id', 'file_size_mb', 'iteration', 'job_id', 'total_time_s', 
                'execution_time_s', 'status', 'total_tasks', 'completed_tasks', 
                'failed_tasks', 'avg_cpu_percent', 'max_memory_mb', 'avg_memory_mb',
                'split_size', 'num_reducers'
            ])
            
            for result in self.results:
                writer.writerow([
                    result['test_id'], result['file_size_mb'], result['iteration'],
                    result['job_id'], result['total_time_seconds'], 
                    result['execution_time_seconds'], result['job_status'],
                    result['total_tasks'], result['completed_tasks'], 
                    result['failed_tasks'], result['avg_cpu_percent'],
                    result['max_memory_mb'], result['avg_memory_mb'],
                    result['split_size'], result['num_reducers']
                ])
                
    def _generate_json_report(self, json_file: Path):
        """Generate JSON performance report."""
        report_data = {
            'test_summary': {
                'timestamp': datetime.now().isoformat(),
                'total_tests': len(self.results),
                'file_sizes_tested': list(set(r['file_size_mb'] for r in self.results)),
                'successful_tests': len([r for r in self.results if r['job_status'] == 'completed']),
                'failed_tests': len([r for r in self.results if r['job_status'] != 'completed'])
            },
            'test_results': self.results
        }
        
        with open(json_file, 'w') as f:
            json.dump(report_data, f, indent=2)
            
    def _generate_html_report(self, html_file: Path):
        """Generate HTML performance report."""
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>GridMR Performance Test Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #f0f0f0; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
        .summary {{ background: #e8f4fd; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .section {{ margin: 20px 0; }}
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        .success {{ color: #4CAF50; }}
        .failure {{ color: #f44336; }}
        .chart {{ margin: 20px 0; }}
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <div class="header">
        <h1>🚀 GridMR Performance Test Report</h1>
        <p><strong>Generated:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        <p><strong>Total Tests:</strong> {len(self.results)}</p>
    </div>
    
    <div class="summary">
        <h2>📊 Test Summary</h2>
        <p><strong>File Sizes Tested:</strong> {', '.join(map(str, sorted(set(r['file_size_mb'] for r in self.results))))} MB</p>
        <p><strong>Successful Tests:</strong> <span class="success">{len([r for r in self.results if r['job_status'] == 'completed'])}</span></p>
        <p><strong>Failed Tests:</strong> <span class="failure">{len([r for r in self.results if r['job_status'] != 'completed'])}</span></p>
    </div>
    
    <div class="section">
        <h2>📈 Performance Results</h2>
        <table>
            <tr>
                <th>Test ID</th>
                <th>File Size (MB)</th>
                <th>Execution Time (s)</th>
                <th>Status</th>
                <th>Tasks (Completed/Total)</th>
                <th>Avg CPU (%)</th>
                <th>Max Memory (MB)</th>
            </tr>
"""
        
        for result in self.results:
            status_class = "success" if result['job_status'] == 'completed' else "failure"
            html_content += f"""
            <tr>
                <td>{result['test_id']}</td>
                <td>{result['file_size_mb']}</td>
                <td>{result['execution_time_seconds']:.2f}</td>
                <td class="{status_class}">{result['job_status']}</td>
                <td>{result['completed_tasks']}/{result['total_tasks']}</td>
                <td>{result['avg_cpu_percent']:.1f}</td>
                <td>{result['max_memory_mb']:.1f}</td>
            </tr>
"""
        
        html_content += """
        </table>
    </div>
    
    <div class="section">
        <h2>📊 Performance Charts</h2>
        <div class="chart">
            <canvas id="executionTimeChart" width="400" height="200"></canvas>
        </div>
        <div class="chart">
            <canvas id="resourceUsageChart" width="400" height="200"></canvas>
        </div>
    </div>
    
    <script>
"""
        
        # Add chart data
        file_sizes = [r['file_size_mb'] for r in self.results if r['job_status'] == 'completed']
        execution_times = [r['execution_time_seconds'] for r in self.results if r['job_status'] == 'completed']
        cpu_usage = [r['avg_cpu_percent'] for r in self.results if r['job_status'] == 'completed']
        memory_usage = [r['max_memory_mb'] for r in self.results if r['job_status'] == 'completed']
        
        html_content += f"""
        // Execution Time Chart
        const ctx1 = document.getElementById('executionTimeChart').getContext('2d');
        new Chart(ctx1, {{
            type: 'line',
            data: {{
                labels: {file_sizes},
                datasets: [{{
                    label: 'Execution Time (s)',
                    data: {execution_times},
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    tension: 0.1
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    title: {{
                        display: true,
                        text: 'Execution Time vs File Size'
                    }}
                }},
                scales: {{
                    x: {{ title: {{ display: true, text: 'File Size (MB)' }} }},
                    y: {{ title: {{ display: true, text: 'Execution Time (s)' }} }}
                }}
            }}
        }});
        
        // Resource Usage Chart
        const ctx2 = document.getElementById('resourceUsageChart').getContext('2d');
        new Chart(ctx2, {{
            type: 'bar',
            data: {{
                labels: {file_sizes},
                datasets: [{{
                    label: 'CPU Usage (%)',
                    data: {cpu_usage},
                    backgroundColor: 'rgba(255, 99, 132, 0.5)',
                    yAxisID: 'y'
                }}, {{
                    label: 'Memory Usage (MB)',
                    data: {memory_usage},
                    backgroundColor: 'rgba(54, 162, 235, 0.5)',
                    yAxisID: 'y1'
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    title: {{
                        display: true,
                        text: 'Resource Usage vs File Size'
                    }}
                }},
                scales: {{
                    x: {{ title: {{ display: true, text: 'File Size (MB)' }} }},
                    y: {{ 
                        type: 'linear',
                        display: true,
                        position: 'left',
                        title: {{ display: true, text: 'CPU Usage (%)' }}
                    }},
                    y1: {{
                        type: 'linear',
                        display: true,
                        position: 'right',
                        title: {{ display: true, text: 'Memory Usage (MB)' }},
                        grid: {{ drawOnChartArea: false }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""
        
        with open(html_file, 'w') as f:
            f.write(html_content)
            
    def _print_summary(self):
        """Print performance summary to console."""
        print("\n" + "=" * 60)
        print("📊 PERFORMANCE SUMMARY")
        print("=" * 60)
        
        successful_results = [r for r in self.results if r['job_status'] == 'completed']
        
        if not successful_results:
            print("❌ No successful tests to summarize")
            return
            
        print(f"✅ Successful tests: {len(successful_results)}/{len(self.results)}")
        print(f"📏 File sizes tested: {sorted(set(r['file_size_mb'] for r in successful_results))} MB")
        
        print("\n📈 Performance Metrics:")
        for size in sorted(set(r['file_size_mb'] for r in successful_results)):
            size_results = [r for r in successful_results if r['file_size_mb'] == size]
            avg_time = sum(r['execution_time_seconds'] for r in size_results) / len(size_results)
            avg_cpu = sum(r['avg_cpu_percent'] for r in size_results) / len(size_results)
            avg_memory = sum(r['max_memory_mb'] for r in size_results) / len(size_results)
            
            print(f"  • {size}MB: {avg_time:.2f}s avg, {avg_cpu:.1f}% CPU, {avg_memory:.1f}MB RAM")

def main():
    """Main function with command line interface."""
    parser = argparse.ArgumentParser(
        description="GridMR Automated Performance Testing Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python automated_performance_test.py --sizes 1 5 10
  python automated_performance_test.py --sizes 0.5 1 2 5 --iterations 3
  python automated_performance_test.py --sizes 1 5 10 --url http://localhost:8080
        """
    )
    
    parser.add_argument(
        '--sizes', nargs='+', type=float, default=[1, 5, 10],
        help='File sizes to test in MB (default: 1 5 10)'
    )
    
    parser.add_argument(
        '--iterations', type=int, default=1,
        help='Number of iterations per file size (default: 1)'
    )
    
    parser.add_argument(
        '--url', default='http://localhost:8000',
        help='GridMR API base URL (default: http://localhost:8000)'
    )
    
    args = parser.parse_args()
    
    # Create test suite
    test_suite = PerformanceTestSuite(args.url)
    
    # Run tests
    test_suite.run_test_suite(args.sizes, args.iterations)

if __name__ == "__main__":
    main()