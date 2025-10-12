#!/usr/bin/env python3
"""
FastAPI S3 Ingestion Benchmark Suite
Comprehensive performance testing with statistical analysis and visualization.
"""
import requests
import json
import time
import random
import string
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from pathlib import Path
import csv

# Configuration
BASE_URL = "http://localhost:8000"
API_KEY = "1rge0nQy6MmiHVv2bA0Lt_8W6eaossBBpmAehTqVHIk"
INGEST_ENDPOINT = f"{BASE_URL}/api/dev/v1/data/ingest"

# Create results directory
RESULTS_DIR = Path("benchmark_results")
RESULTS_DIR.mkdir(exist_ok=True)

def generate_benchmark_data(num_items):
    """Generate test data with specified number of items."""
    def random_string(length=8):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def random_email():
        return f"{random_string(6)}@{random_string(5)}.com"
    
    data = {
        "batch_id": random_string(12),
        "timestamp": datetime.now().isoformat(),
        "items": []
    }
    
    for i in range(num_items):
        item = {
            "id": random.randint(1000, 9999),
            "type": random.choice(["event", "user_action", "system_log"]),
            "user_id": random.randint(10000, 99999),
            "properties": {
                "email": random_email(),
                "source": random.choice(["web", "mobile", "api"]),
                "timestamp": datetime.now().isoformat(),
                "session_id": random_string(16)
            },
            "metrics": {
                "value": round(random.uniform(1, 1000), 2),
                "duration": random.randint(100, 5000)
            }
        }
        data["items"].append(item)
    
    return data

def single_request_benchmark(payload, request_id):
    """Perform a single request and return timing data."""
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    start_time = time.time()
    try:
        response = requests.post(
            INGEST_ENDPOINT,
            json=payload,
            headers=headers,
            timeout=30
        )
        end_time = time.time()
        
        return {
            "request_id": request_id,
            "response_time": end_time - start_time,
            "status_code": response.status_code,
            "success": response.status_code == 200,
            "payload_size": len(json.dumps(payload).encode('utf-8'))
        }
    except Exception as e:
        end_time = time.time()
        return {
            "request_id": request_id,
            "response_time": end_time - start_time,
            "status_code": 0,
            "success": False,
            "error": str(e),
            "payload_size": len(json.dumps(payload).encode('utf-8'))
        }

def calculate_statistics(response_times):
    """Calculate detailed statistics from response times."""
    if not response_times:
        return {}
    
    response_times_ms = [rt * 1000 for rt in response_times]  # Convert to milliseconds
    
    return {
        "count": len(response_times_ms),
        "mean": np.mean(response_times_ms),
        "median": np.median(response_times_ms),
        "std": np.std(response_times_ms),
        "min": np.min(response_times_ms),
        "max": np.max(response_times_ms),
        "p50": np.percentile(response_times_ms, 50),
        "p90": np.percentile(response_times_ms, 90),
        "p95": np.percentile(response_times_ms, 95),
        "p99": np.percentile(response_times_ms, 99),
        "p99_9": np.percentile(response_times_ms, 99.9)
    }

def run_benchmark_suite():
    """Run comprehensive benchmark suite with different payload sizes."""
    
    # Test configurations: (num_items, num_requests, description)
    test_configs = [
        (1, 1000, "1 item per request"),
        (10, 1000, "10 items per request"),
        (100, 1000, "100 items per request"),
        (1000, 1000, "1000 items per request"),
        (10000, 1000, "10000 items per request")
    ]
    
    results = {}
    all_response_times = []  # For detailed analysis
    
    print("🚀 Starting Comprehensive Benchmark Suite")
    print("=" * 60)
    
    for num_items, num_requests, description in test_configs:
        print(f"\n📊 Testing: {description}")
        print(f"   Items per request: {num_items}")
        print(f"   Number of requests: {num_requests}")
        
        # Generate sample payload to calculate size
        sample_payload = generate_benchmark_data(num_items)
        payload_size_bytes = len(json.dumps(sample_payload).encode('utf-8'))
        payload_size_mb = payload_size_bytes / (1024 * 1024)
        
        print(f"   Payload size: {payload_size_mb:.3f} MB ({payload_size_bytes:,} bytes)")
        
        # Store request results
        request_results = []
        successful_requests = 0
        failed_requests = 0
        
        # Progress tracking
        completed = 0
        print(f"   Progress: 0/{num_requests}", end="", flush=True)
        
        start_benchmark = time.time()
        
        # Sequential requests
        for i in range(num_requests):
            payload = generate_benchmark_data(num_items)
            result = single_request_benchmark(payload, i + 1)
            request_results.append(result)
            
            if result['success']:
                successful_requests += 1
                # Store for detailed analysis
                all_response_times.append({
                    'items': num_items,
                    'request_id': i + 1,
                    'response_time_ms': result['response_time'] * 1000,
                    'payload_size_mb': payload_size_mb
                })
            else:
                failed_requests += 1
            
            completed += 1
            
            # Update progress every 100 requests
            if completed % 100 == 0 or completed == num_requests:
                print(f"\r   Progress: {completed}/{num_requests}", end="", flush=True)
        
        end_benchmark = time.time()
        total_benchmark_time = end_benchmark - start_benchmark
        
        print()  # New line after progress
        
        # Extract successful response times for statistics
        successful_times = [r['response_time'] for r in request_results if r['success']]
        
        if successful_times:
            stats = calculate_statistics(successful_times)
            
            # Calculate throughput
            total_data_mb = (payload_size_mb * successful_requests)
            throughput_mbps = total_data_mb / total_benchmark_time
            rps = successful_requests / total_benchmark_time
            
            # Store results
            results[f"{num_items}_items"] = {
                "config": {"items": num_items, "requests": num_requests},
                "payload_size_mb": payload_size_mb,
                "successful_requests": successful_requests,
                "failed_requests": failed_requests,
                "total_time": total_benchmark_time,
                "throughput_mbps": throughput_mbps,
                "requests_per_second": rps,
                "statistics": stats
            }
            
            # Print results
            print(f"   ✅ Completed: {successful_requests}/{num_requests} successful")
            print(f"   ⚡ Total time: {total_benchmark_time:.2f}s")
            print(f"   🚀 Throughput: {throughput_mbps:.2f} MB/s")
            print(f"   📈 Requests/sec: {rps:.1f}")
            print(f"   📊 P95: {stats['p95']:.1f}ms, P99: {stats['p99']:.1f}ms")
            
        else:
            print(f"   ❌ All requests failed!")
            results[f"{num_items}_items"] = {
                "config": {"items": num_items, "requests": num_requests},
                "successful_requests": 0,
                "failed_requests": failed_requests,
                "error": "All requests failed"
            }
        
        # Small delay between test suites
        time.sleep(2)
    
    print(f"\n🏁 Benchmark Suite Completed!")
    return results, all_response_times

def save_results_to_csv(results, all_response_times):
    """Save benchmark results to CSV files."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save summary results
    summary_file = RESULTS_DIR / f"benchmark_summary_{timestamp}.csv"
    summary_data = []
    
    for key, result in results.items():
        if 'statistics' in result:
            config = result['config']
            stats = result['statistics']
            
            summary_data.append({
                'items_per_request': config['items'],
                'total_requests': config['requests'],
                'successful_requests': result['successful_requests'],
                'payload_size_mb': result['payload_size_mb'],
                'requests_per_second': result['requests_per_second'],
                'throughput_mbps': result['throughput_mbps'],
                'mean_ms': stats['mean'],
                'median_ms': stats['median'],
                'p90_ms': stats['p90'],
                'p95_ms': stats['p95'],
                'p99_ms': stats['p99'],
                'p99_9_ms': stats['p99_9'],
                'min_ms': stats['min'],
                'max_ms': stats['max'],
                'std_ms': stats['std']
            })
    
    pd.DataFrame(summary_data).to_csv(summary_file, index=False)
    print(f"📄 Summary saved to: {summary_file}")
    
    # Save detailed response times
    detail_file = RESULTS_DIR / f"benchmark_details_{timestamp}.csv"
    pd.DataFrame(all_response_times).to_csv(detail_file, index=False)
    print(f"📄 Detailed data saved to: {detail_file}")
    
    return summary_file, detail_file

def create_visualizations(results, all_response_times):
    """Create comprehensive visualizations of benchmark results."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create a large figure with multiple subplots
    fig = plt.figure(figsize=(20, 16))
    
    # Prepare data for plotting
    items_list = []
    mean_times = []
    p95_times = []
    p99_times = []
    throughput_list = []
    rps_list = []
    payload_sizes = []
    
    for key, result in results.items():
        if 'statistics' in result:
            config = result['config']
            stats = result['statistics']
            items_list.append(config['items'])
            mean_times.append(stats['mean'])
            p95_times.append(stats['p95'])
            p99_times.append(stats['p99'])
            throughput_list.append(result['throughput_mbps'])
            rps_list.append(result['requests_per_second'])
            payload_sizes.append(result['payload_size_mb'])
    
    # Plot 1: Response Time vs Payload Size (log scale)
    plt.subplot(2, 3, 1)
    plt.loglog(items_list, mean_times, 'o-', label='Mean', linewidth=2, markersize=8)
    plt.loglog(items_list, p95_times, 's-', label='P95', linewidth=2, markersize=8)
    plt.loglog(items_list, p99_times, '^-', label='P99', linewidth=2, markersize=8)
    plt.xlabel('Items per Request')
    plt.ylabel('Response Time (ms)')
    plt.title('Response Time vs Payload Size')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Plot 2: Throughput vs Payload Size
    plt.subplot(2, 3, 2)
    plt.semilogx(items_list, throughput_list, 'o-', color='green', linewidth=2, markersize=8)
    plt.xlabel('Items per Request')
    plt.ylabel('Throughput (MB/s)')
    plt.title('Throughput vs Payload Size')
    plt.grid(True, alpha=0.3)
    
    # Plot 3: Requests per Second vs Payload Size
    plt.subplot(2, 3, 3)
    plt.semilogx(items_list, rps_list, 'o-', color='red', linewidth=2, markersize=8)
    plt.xlabel('Items per Request')
    plt.ylabel('Requests per Second')
    plt.title('Request Rate vs Payload Size')
    plt.grid(True, alpha=0.3)
    
    # Plot 4: Response Time Distribution (Box Plot)
    plt.subplot(2, 3, 4)
    df_responses = pd.DataFrame(all_response_times)
    if not df_responses.empty:
        sns.boxplot(data=df_responses, x='items', y='response_time_ms')
        plt.xlabel('Items per Request')
        plt.ylabel('Response Time (ms)')
        plt.title('Response Time Distribution')
        plt.yscale('log')
    
    # Plot 5: Performance Summary Bar Chart
    plt.subplot(2, 3, 5)
    x_pos = np.arange(len(items_list))
    width = 0.35
    
    plt.bar(x_pos - width/2, mean_times, width, label='Mean Response Time (ms)', alpha=0.8)
    plt.bar(x_pos + width/2, np.array(rps_list)*10, width, label='RPS x10', alpha=0.8)
    
    plt.xlabel('Test Configuration')
    plt.ylabel('Value')
    plt.title('Performance Summary')
    plt.xticks(x_pos, [f'{x} items' for x in items_list], rotation=45)
    plt.legend()
    plt.yscale('log')
    
    # Plot 6: Payload Size vs Performance Metrics
    plt.subplot(2, 3, 6)
    plt.loglog(payload_sizes, mean_times, 'o-', label='Mean Response Time', linewidth=2)
    plt.loglog(payload_sizes, np.array(throughput_list)*100, 's-', label='Throughput x100', linewidth=2)
    plt.xlabel('Payload Size (MB)')
    plt.ylabel('Value')
    plt.title('Payload Size Impact')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    plot_file = RESULTS_DIR / f"benchmark_plots_{timestamp}.png"
    plt.savefig(plot_file, dpi=300, bbox_inches='tight')
    print(f"📊 Plots saved to: {plot_file}")
    
    # Show the plot
    plt.show()
    
    return plot_file

def main():
    """Main benchmark execution."""
    print("🚀 FastAPI S3 Ingestion Benchmark Suite")
    print("=" * 50)
    
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/api/dev/v1/health", timeout=5)
        if response.status_code != 200:
            print("❌ Server health check failed!")
            return
    except:
        print("❌ Cannot connect to server! Make sure it's running on http://localhost:8000")
        return
    
    print("✅ Server is running, starting benchmark...")
    
    # Run benchmarks
    results, all_response_times = run_benchmark_suite()
    
    # Save results
    summary_file, detail_file = save_results_to_csv(results, all_response_times)
    
    # Create visualizations
    plot_file = create_visualizations(results, all_response_times)
    
    print(f"\n🎉 Benchmark completed successfully!")
    print(f"📁 Results saved in: {RESULTS_DIR}")
    print(f"   - Summary: {summary_file.name}")
    print(f"   - Details: {detail_file.name}")
    print(f"   - Plots: {plot_file.name}")

if __name__ == "__main__":
    main()