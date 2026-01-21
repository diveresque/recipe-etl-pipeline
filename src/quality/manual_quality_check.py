"""
Manual quality check script for recipe data.

Run from command line:
    python -m src.quality.manual_quality_check
    
Or from project root:
    python src/quality/manual_quality_check.py
"""
import pandas as pd
from pathlib import Path
from src.quality.data_quality import DataQualityChecker

def main():
    parquet_path = Path("data/processed/recipes.parquet")
    
    if not parquet_path.exists():
        print(f"Error: Parquet file not found at {parquet_path}")
        print("Please run the ETL pipeline first to generate data.")
        return
    
    print(f"Loading data from {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df)} recipes\n")
    
    checker = DataQualityChecker()
    checker.check_recipe_data_quality(df)
    checker.log_summary()
    
    # Get summary as dict
    summary = checker.get_summary()
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total Checks: {summary['total_checks']}")
    print(f"Passed: {summary['passed']}")
    print(f"Failed: {summary['failed']}")
    print(f"Pass Rate: {summary['pass_rate']:.2%}")
    
    # Show detailed failure information
    failed_checks = checker.get_failed_checks()
    if failed_checks:
        print("\n" + "=" * 60)
        print("FAILED CHECKS - DETAILED INFORMATION")
        print("=" * 60)
        for i, result in enumerate(failed_checks, 1):
            print(f"\n{i}. {result.check_name}")
            print(f"   Status: ✗ FAILED")
            print(f"   Message: {result.message}")
            
            if result.threshold is not None and result.metric_value is not None:
                if result.threshold < 1:  # Percentage threshold
                    print(f"   Expected: >= {result.threshold:.2%} ({result.threshold:.4f})")
                    print(f"   Actual: {result.metric_value:.2%} ({result.metric_value:.4f})")
                    shortfall = result.threshold - result.metric_value
                    if shortfall > 0:
                        print(f"   Shortfall: {shortfall:.2%} ({shortfall:.4f})")
                else:  # Count threshold
                    print(f"   Expected: >= {result.threshold}")
                    print(f"   Actual: {result.metric_value}")
                    shortfall = result.threshold - result.metric_value
                    if shortfall > 0:
                        print(f"   Shortfall: {shortfall}")
            elif result.metric_value is not None:
                print(f"   Metric Value: {result.metric_value}")
            
            if result.threshold is None:
                print(f"   Note: This check does not have a threshold (binary check)")
        
        print("\n" + "=" * 60)
    
    return summary

if __name__ == "__main__":
    main()