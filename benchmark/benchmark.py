import concurrent.futures
import json
import random
import statistics
import string
import time
from typing import List, Dict

import httpx
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm


class KVStoreBenchmark:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.client = httpx.Client(timeout=30.0)
        self.results: Dict[str, List[float]] = {}

    @staticmethod
    def generate_random_string(length: int = 10) -> str:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def generate_random_data(self, num_items: int, value_size: int = 100) -> List[tuple]:
        return [(self.generate_random_string(), self.generate_random_string(value_size))
                for _ in range(num_items)]

    def benchmark_single_puts(self, num_operations: int = 1000) -> Dict[str, float]:
        latencies = []
        successful = 0
        failures = []
        retry_delay = 1.0

        print(f"\nBenchmarking {num_operations} single PUT operations...")
        start_time = time.time()

        for i in tqdm(range(num_operations)):
            key = self.generate_random_string()
            value = self.generate_random_string(100)

            # Try up to 3 times with exponential backoff
            for retry in range(3):
                try:
                    op_start = time.time()
                    response = self.client.put(
                        f"{self.base_url}/kv/{key}",
                        json=value,
                        timeout=30.0
                    )
                    latency = time.time() - op_start

                    if response.status_code == 200:
                        successful += 1
                        latencies.append(latency)
                        break
                    elif response.status_code == 503:
                        if retry < 2:
                            time.sleep(retry_delay * (2 ** retry))
                        failures.append({
                            'attempt': retry + 1,
                            'status': response.status_code,
                            'key': key,
                            'error': response.text
                        })
                    else:
                        failures.append({
                            'attempt': retry + 1,
                            'status': response.status_code,
                            'key': key,
                            'error': response.text
                        })
                        break

                except Exception as e:
                    failures.append({
                        'attempt': retry + 1,
                        'key': key,
                        'error': str(e)
                    })
                    if retry < 2:
                        time.sleep(retry_delay * (2 ** retry))

        total_time = time.time() - start_time

        if not latencies:
            print("\nWarning: No successful operations recorded!")
            print(f"Total failures: {len(failures)}")
            print("\nFirst 5 failures:")
            for f in failures[:5]:
                print(f"  {f}")

            return {
                "operations": num_operations,
                "successful": 0,
                "failed": len(failures),
                "total_time": total_time,
                "ops_per_second": 0,
                "avg_latency": None,
                "p95_latency": None,
                "p99_latency": None,
                "failure_sample": failures[:5]
            }

        results = {
            "operations": num_operations,
            "successful": successful,
            "failed": len(failures),
            "total_time": total_time,
            "ops_per_second": successful / total_time,
            "avg_latency": statistics.mean(latencies),
            "p95_latency": statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else None,
            "p99_latency": statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else None,
            "single_puts": latencies,
            "failure_sample": failures[:5] if failures else None
        }

        return results

    def benchmark_batch_puts(self, batch_sizes=None) -> Dict[str, Dict[str, float]]:
        if batch_sizes is None:
            batch_sizes = [10, 50, 100, 500]
        results = {}

        for batch_size in batch_sizes:
            print(f"\nBenchmarking batch PUT with size {batch_size}...")
            latencies = []
            successful = 0
            num_batches = 1000 // batch_size

            start_time = time.time()

            for _ in tqdm(range(num_batches)):
                items = [
                    {"key": self.generate_random_string(),
                     "value": self.generate_random_string(100)}
                    for _ in range(batch_size)
                ]

                op_start = time.time()
                response = self.client.put(f"{self.base_url}/kv/batch",
                                           json={"items": items})
                latency = time.time() - op_start

                if response.status_code == 200:
                    successful += 1
                    latencies.append(latency)

            total_time = time.time() - start_time

            results[batch_size] = {
                "operations": num_batches,
                "items_processed": num_batches * batch_size,
                "successful_batches": successful,
                "total_time": total_time,
                "batches_per_second": successful / total_time,
                "items_per_second": (successful * batch_size) / total_time,
                "avg_latency": statistics.mean(latencies),
                "p95_latency": statistics.quantiles(latencies, n=20)[18],
                "p99_latency": statistics.quantiles(latencies, n=100)[98]
            }

            self.results[f"batch_puts_{batch_size}"] = latencies

        return results

    def benchmark_gets(self, num_operations: int = 1000) -> Dict[str, float]:
        # First, put some data
        print("\nPreparing data for GET benchmark...")
        keys = []
        for _ in tqdm(range(num_operations)):
            key = self.generate_random_string()
            value = self.generate_random_string(100)
            response = self.client.put(f"{self.base_url}/kv/{key}", json=value)
            if response.status_code == 200:
                keys.append(key)

        latencies = []
        successful = 0

        print(f"\nBenchmarking {num_operations} GET operations...")
        start_time = time.time()

        for key in tqdm(keys):
            op_start = time.time()
            response = self.client.get(f"{self.base_url}/kv/{key}")
            latency = time.time() - op_start

            if response.status_code == 200:
                successful += 1
                latencies.append(latency)

        total_time = time.time() - start_time

        results = {"operations": num_operations, "successful": successful, "total_time": total_time,
                   "ops_per_second": successful / total_time, "avg_latency": statistics.mean(latencies),
                   "p95_latency": statistics.quantiles(latencies, n=20)[18],
                   "p99_latency": statistics.quantiles(latencies, n=100)[98], "gets": latencies}

        return results

    def benchmark_range_queries(self,
                                range_sizes=None) -> Dict[str, Dict[str, float]]:
        if range_sizes is None:
            range_sizes = [10, 50, 100, 500]
        results = {}

        # Prepare sorted data
        print("\nPreparing data for range query benchmark...")
        keys = sorted([self.generate_random_string() for _ in range(1000)])
        for key in tqdm(keys):
            value = self.generate_random_string(100)
            self.client.put(f"{self.base_url}/kv/{key}", json=value)

        for range_size in range_sizes:
            print(f"\nBenchmarking range queries of size {range_size}...")
            latencies = []
            successful = 0
            num_queries = 100

            start_time = time.time()

            for _ in tqdm(range(num_queries)):
                start_idx = random.randint(0, len(keys) - range_size - 1)

                op_start = time.time()
                response = self.client.post(
                    f"{self.base_url}/kv/range",
                    json={
                        "start_key": keys[start_idx],
                        "end_key": keys[start_idx + range_size]
                    }
                )
                latency = time.time() - op_start

                if response.status_code == 200:
                    successful += 1
                    latencies.append(latency)

            total_time = time.time() - start_time

            results[range_size] = {
                "operations": num_queries,
                "successful": successful,
                "total_time": total_time,
                "queries_per_second": successful / total_time,
                "avg_latency": statistics.mean(latencies),
                "p95_latency": statistics.quantiles(latencies, n=20)[18],
                "p99_latency": statistics.quantiles(latencies, n=100)[98]
            }

            self.results[f"range_query_{range_size}"] = latencies

        return results
    
    def benchmark_concurrent_clients(self, num_clients=None) -> Dict[str, Dict[str, float]]:
        if num_clients is None:
            num_clients = [5, 10, 25, 50, 100]
        results = {}

        def client_worker(client_id: int, num_ops: int = 100):
            client = httpx.Client(timeout=30.0)
            latencies = []
            successful_workers = 0

            for _ in range(num_ops):
                key = self.generate_random_string()
                value = self.generate_random_string(100)

                op_start = time.time()
                response = client.put(f"{self.base_url}/kv/{key}", json=value)
                latency = time.time() - op_start

                if response.status_code == 200:
                    successful_workers += 1
                    latencies.append(latency)

            return successful_workers, latencies

        for num_client in num_clients:
            print(f"\nBenchmarking with {num_client} concurrent clients...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_client) as executor:
                # Start timing BEFORE submitting tasks
                start_time = time.time()

                futures = [
                    executor.submit(client_worker, i)
                    for i in range(num_client)
                ]

                # Wait for completion and gather results
                concurrent.futures.wait(futures)
                total_time = time.time() - start_time

                all_latencies = []
                total_successful = 0

                for future in futures:
                    successful, latencies = future.result()
                    total_successful += successful
                    all_latencies.extend(latencies)

            # Guard against zero division
            if total_time <= 0:
                total_time = 0.001  # Set a minimum time to prevent division by zero

            results[num_client] = {
                "total_operations": num_client * 100,
                "successful_operations": total_successful,
                "total_time": total_time,
                "ops_per_second": total_successful / total_time if total_time > 0 else 0,
                "avg_latency": statistics.mean(all_latencies) if all_latencies else 0,
                "p95_latency": statistics.quantiles(all_latencies, n=20)[18] if len(all_latencies) >= 20 else None,
                "p99_latency": statistics.quantiles(all_latencies, n=100)[98] if len(all_latencies) >= 100 else None
            }

            self.results[f"concurrent_{num_client}"] = all_latencies

        return results

    def plot_results(self, output_dir: str = "benchmark_results"):
        import os
        os.makedirs(output_dir, exist_ok=True)

        # Plot latency distributions
        plt.figure(figsize=(15, 10))
        for operation, latencies in self.results.items():
            sns.kdeplot(latencies, label=operation)
        plt.title("Operation Latency Distributions")
        plt.xlabel("Latency (seconds)")
        plt.ylabel("Density")
        plt.legend()
        plt.savefig(f"{output_dir}/latency_distributions.png")
        plt.close()


def run_all_benchmarks():
    benchmark = KVStoreBenchmark()

    # Run all benchmarks
    single_put_results = benchmark.benchmark_single_puts()
    batch_results = benchmark.benchmark_batch_puts()
    get_results = benchmark.benchmark_gets()
    range_results = benchmark.benchmark_range_queries()
    concurrent_results = benchmark.benchmark_concurrent_clients()

    # Generate plots
    benchmark.plot_results()

    # Save results
    all_results = {
        "single_put": single_put_results,
        "batch_put": batch_results,
        "get": get_results,
        "range_query": range_results,
        "concurrent": concurrent_results
    }

    with open("benchmark_results/results.json", "w") as f:
        json.dump(all_results, f, indent=2)

    print("\nBenchmark Summary:")
    print(f"Single PUT operations per second: {single_put_results['ops_per_second']:.2f}")
    print(f"GET operations per second: {get_results['ops_per_second']:.2f}")
    print("\nBatch PUT performance:")
    for batch_size, results in batch_results.items():
        print(f"  Batch size {batch_size}: {results['items_per_second']:.2f} items/second")
    print("\nRange query performance:")
    for range_size, results in range_results.items():
        print(f"  Range size {range_size}: {results['queries_per_second']:.2f} queries/second")
    print("\nConcurrent client performance:")
    for num_clients, results in concurrent_results.items():
        print(f"  {num_clients} clients: {results['ops_per_second']:.2f} ops/second")


if __name__ == "__main__":
    run_all_benchmarks()
