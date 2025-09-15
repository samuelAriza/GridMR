#!/usr/bin/env python3
"""
Test File Generator for GridMR Performance Testing
==================================================
This script generates text files of specific sizes for MapReduce performance testing.
The generated files contain realistic text content suitable for word count operations.
"""

import os
import sys
import argparse
import random
from pathlib import Path

# Sample words for generating realistic text content
SAMPLE_WORDS = [
    "mapreduce", "distributed", "computing", "performance", "analysis", "data",
    "processing", "parallel", "algorithm", "cluster", "node", "worker", "master",
    "task", "job", "scheduler", "partition", "reduce", "map", "function",
    "hadoop", "spark", "framework", "bigdata", "analytics", "scalable",
    "fault", "tolerant", "throughput", "latency", "optimization", "efficiency",
    "system", "architecture", "design", "implementation", "execution", "monitoring",
    "metrics", "benchmark", "evaluation", "testing", "validation", "verification",
    "python", "java", "scala", "programming", "development", "software",
    "engineering", "technology", "innovation", "research", "university", "project",
    "experiment", "study", "results", "conclusion", "recommendation", "future",
    "work", "improvement", "enhancement", "modification", "extension", "integration"
]

# Sample sentences for more realistic text structure
SAMPLE_SENTENCES = [
    "MapReduce is a programming model for processing large datasets in parallel.",
    "The distributed computing framework enables scalable data processing across clusters.",
    "Performance analysis shows significant improvements in processing throughput.",
    "Worker nodes execute map and reduce tasks efficiently across the cluster.",
    "The master node coordinates job scheduling and task distribution.",
    "Fault tolerance mechanisms ensure system reliability during failures.",
    "Data partitioning strategies optimize load balancing across workers.",
    "Monitoring metrics provide insights into system performance characteristics.",
    "Benchmark results demonstrate the effectiveness of the optimization techniques.",
    "The experimental evaluation validates the proposed algorithmic improvements."
]

def generate_text_content(target_size_bytes):
    """
    Generate realistic text content of approximately the target size.
    
    Args:
        target_size_bytes (int): Target file size in bytes
        
    Returns:
        str: Generated text content
    """
    content = []
    current_size = 0
    line_number = 1
    
    print(f"Generating text content for {target_size_bytes:,} bytes...")
    
    while current_size < target_size_bytes:
        # Choose content generation strategy
        if random.random() < 0.7:  # 70% sentences, 30% word lists
            # Generate sentence-based content
            sentence = random.choice(SAMPLE_SENTENCES)
            # Add some variation with word substitution
            if random.random() < 0.3:
                words = sentence.split()
                if len(words) > 3:
                    replace_idx = random.randint(1, len(words) - 2)
                    words[replace_idx] = random.choice(SAMPLE_WORDS)
                    sentence = " ".join(words)
            
            line = f"Line {line_number:06d}: {sentence}"
        else:
            # Generate word list content
            num_words = random.randint(5, 15)
            words = random.sample(SAMPLE_WORDS, min(num_words, len(SAMPLE_WORDS)))
            line = f"Data {line_number:06d}: {' '.join(words)}"
        
        # Add timestamp occasionally for uniqueness
        if random.random() < 0.1:
            import time
            line += f" [timestamp: {int(time.time() * 1000000)}]"
        
        line += "\n"
        content.append(line)
        current_size += len(line.encode('utf-8'))
        line_number += 1
        
        # Progress indicator for large files
        if line_number % 1000 == 0:
            progress = (current_size / target_size_bytes) * 100
            print(f"Progress: {progress:.1f}% ({current_size:,}/{target_size_bytes:,} bytes)")
    
    return "".join(content)

def create_test_file(size_mb, output_dir, filename="input.txt"):
    """
    Create a test file with the specified size.
    
    Args:
        size_mb (float): File size in megabytes
        output_dir (str): Output directory path
        filename (str): Output filename
        
    Returns:
        str: Path to the created file
    """
    # Convert MB to bytes
    target_size_bytes = int(size_mb * 1024 * 1024)
    
    # Ensure output directory exists
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Full file path
    file_path = output_path / filename
    
    print(f"Creating test file: {file_path}")
    print(f"Target size: {size_mb} MB ({target_size_bytes:,} bytes)")
    
    # Generate content
    content = generate_text_content(target_size_bytes)
    
    # Write to file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    # Verify actual file size
    actual_size = file_path.stat().st_size
    actual_size_mb = actual_size / (1024 * 1024)
    
    print(f"File created successfully!")
    print(f"Actual size: {actual_size_mb:.2f} MB ({actual_size:,} bytes)")
    print(f"Path: {file_path.absolute()}")
    
    # Generate file statistics
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = sum(1 for _ in f)
    
    with open(file_path, 'r', encoding='utf-8') as f:
        words = sum(len(line.split()) for line in f)
    
    print(f"File statistics:")
    print(f"  Lines: {lines:,}")
    print(f"  Words: {words:,}")
    print(f"  Characters: {actual_size:,}")
    
    return str(file_path.absolute())

def parse_size(size_str):
    """
    Parse size string with optional unit suffix.
    
    Args:
        size_str (str): Size string (e.g., "5", "10MB", "2.5GB")
        
    Returns:
        float: Size in megabytes
    """
    size_str = size_str.upper().strip()
    
    # Extract number and unit
    if size_str.endswith('GB'):
        return float(size_str[:-2]) * 1024
    elif size_str.endswith('MB'):
        return float(size_str[:-2])
    elif size_str.endswith('KB'):
        return float(size_str[:-2]) / 1024
    elif size_str.endswith('B'):
        return float(size_str[:-1]) / (1024 * 1024)
    else:
        # Assume MB if no unit specified
        return float(size_str)

def main():
    """Main function with command line interface."""
    
    parser = argparse.ArgumentParser(
        description="Generate test files of specific sizes for GridMR performance testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_test_file.py 5                    # 5MB file
  python generate_test_file.py 10MB                 # 10MB file
  python generate_test_file.py 2.5GB                # 2.5GB file
  python generate_test_file.py 1024KB               # 1024KB file
  python generate_test_file.py 5 --output /tmp      # Custom output directory
  python generate_test_file.py 5 --name large.txt   # Custom filename
        """
    )
    
    parser.add_argument(
        'size',
        help='File size (e.g., 5, 10MB, 2.5GB, 1024KB). Default unit is MB.'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='data',
        help='Output directory (default: data)'
    )
    
    parser.add_argument(
        '--name', '-n',
        default='input.txt',
        help='Output filename (default: input.txt)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    try:
        # Parse size
        size_mb = parse_size(args.size)
        
        if size_mb <= 0:
            raise ValueError("Size must be positive")
        
        if size_mb > 10240:  # 10GB limit
            print("Warning: Creating files larger than 10GB may take a long time.")
            response = input("Continue? (y/N): ")
            if response.lower() != 'y':
                print("Operation cancelled.")
                return 1
        
        # Create the file
        file_path = create_test_file(size_mb, args.output, args.name)
        
        print(f"\n✅ Success! Test file created at: {file_path}")
        
        return 0
        
    except ValueError as e:
        print(f"Error: Invalid size format: {e}")
        print("Use formats like: 5, 10MB, 2.5GB, 1024KB")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())