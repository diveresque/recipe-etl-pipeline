# src/quality/data_quality.py
"""
Data Quality Checks Module

This module provides data quality validation functions that can be used
throughout the ETL pipeline to ensure data integrity and completeness.
"""
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class QualityCheckResult:
    """Result of a data quality check"""
    check_name: str
    passed: bool
    message: str
    metric_value: Optional[float] = None
    threshold: Optional[float] = None

def normalize_ingredients(val):
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if hasattr(val, "tolist"):      # numpy / pyarrow
        return val.tolist()
    return []

class DataQualityChecker:
    """Main class for running data quality checks"""
    
    def __init__(self):
        self.results: List[QualityCheckResult] = []
    
    def check_not_null(self, df: pd.DataFrame, column: str, threshold: float = 0.95) -> QualityCheckResult:
        """
        Check that a column has at least threshold% non-null values.
        
        Args:
            df: DataFrame to check
            column: Column name to check
            threshold: Minimum proportion of non-null values (default 0.95)
        
        Returns:
            QualityCheckResult
        """
        total_rows = len(df)
        if total_rows == 0:
            return QualityCheckResult(
                check_name=f"not_null_{column}",
                passed=False,
                message=f"DataFrame is empty",
                metric_value=0.0,
                threshold=threshold
            )
        
        non_null_count = df[column].notna().sum()
        non_null_proportion = non_null_count / total_rows
        
        passed = non_null_proportion >= threshold
        
        result = QualityCheckResult(
            check_name=f"not_null_{column}",
            passed=passed,
            message=f"Column '{column}': {non_null_count}/{total_rows} non-null ({non_null_proportion:.2%})",
            metric_value=non_null_proportion,
            threshold=threshold
        )
        
        self.results.append(result)
        return result
    
    def check_unique(self, df: pd.DataFrame, column: str) -> QualityCheckResult:
        """
        Check that a column has unique values (or within acceptable duplicate rate).
        
        Args:
            df: DataFrame to check
            column: Column name to check
        
        Returns:
            QualityCheckResult
        """
        total_rows = len(df)
        if total_rows == 0:
            return QualityCheckResult(
                check_name=f"unique_{column}",
                passed=False,
                message="DataFrame is empty",
                metric_value=0.0
            )
        
        unique_count = df[column].nunique()
        duplicate_count = total_rows - unique_count
        duplicate_rate = duplicate_count / total_rows if total_rows > 0 else 0
        
        passed = duplicate_rate == 0
        
        result = QualityCheckResult(
            check_name=f"unique_{column}",
            passed=passed,
            message=f"Column '{column}': {unique_count} unique values, {duplicate_count} duplicates ({duplicate_rate:.2%})",
            metric_value=duplicate_rate
        )
        
        self.results.append(result)
        return result
    
    def check_value_range(self, df: pd.DataFrame, column: str, min_value: Optional[float] = None, 
                         max_value: Optional[float] = None) -> QualityCheckResult:
        """
        Check that column values are within a specified range.
        
        Args:
            df: DataFrame to check
            column: Column name to check
            min_value: Minimum allowed value (inclusive)
            max_value: Maximum allowed value (inclusive)
        
        Returns:
            QualityCheckResult
        """
        if column not in df.columns:
            return QualityCheckResult(
                check_name=f"range_{column}",
                passed=False,
                message=f"Column '{column}' does not exist"
            )
        
        numeric_df = pd.to_numeric(df[column], errors='coerce')
        valid_rows = numeric_df.notna()
        
        if not valid_rows.any():
            return QualityCheckResult(
                check_name=f"range_{column}",
                passed=False,
                message=f"Column '{column}' has no numeric values"
            )
        
        violations = []
        if min_value is not None:
            below_min = (numeric_df < min_value).sum()
            if below_min > 0:
                violations.append(f"{below_min} values below minimum {min_value}")
        
        if max_value is not None:
            above_max = (numeric_df > max_value).sum()
            if above_max > 0:
                violations.append(f"{above_max} values above maximum {max_value}")
        
        passed = len(violations) == 0
        
        result = QualityCheckResult(
            check_name=f"range_{column}",
            passed=passed,
            message=f"Column '{column}': " + ("; ".join(violations) if violations else "All values within range"),
            metric_value=len(violations)
        )
        
        self.results.append(result)
        return result
    
    def check_record_count(self, df: pd.DataFrame, min_count: int = 1, 
                          max_count: Optional[int] = None) -> QualityCheckResult:
        """
        Check that DataFrame has an acceptable number of records.
        
        Args:
            df: DataFrame to check
            min_count: Minimum number of records required
            max_count: Maximum number of records allowed (optional)
        
        Returns:
            QualityCheckResult
        """
        count = len(df)
        violations = []
        
        if count < min_count:
            violations.append(f"Record count {count} is below minimum {min_count}")
        
        if max_count is not None and count > max_count:
            violations.append(f"Record count {count} exceeds maximum {max_count}")
        
        passed = len(violations) == 0
        
        result = QualityCheckResult(
            check_name="record_count",
            passed=passed,
            message=f"Record count: {count}" + ("; " + "; ".join(violations) if violations else ""),
            metric_value=float(count)
        )
        
        self.results.append(result)
        return result
    
    def check_recipe_data_quality(self, df: pd.DataFrame) -> Dict[str, QualityCheckResult]:
        """
        Run a comprehensive set of quality checks specific to recipe data.
        
        Args:
            df: Recipe DataFrame to validate
        
        Returns:
            Dictionary of check results
        """
        results = {}
        
        # Check record count
        results['record_count'] = self.check_record_count(df, min_count=1)
        
        # Check required columns exist
        required_columns = ['source_name', 'source_id', 'name']
        for col in required_columns:
            if col in df.columns:
                results[f'not_null_{col}'] = self.check_not_null(df, col, threshold=1.0)
            else:
                results[f'missing_column_{col}'] = QualityCheckResult(
                    check_name=f'missing_column_{col}',
                    passed=False,
                    message=f"Required column '{col}' is missing"
                )
                self.results.append(results[f'missing_column_{col}'])
        
        # Check uniqueness of source_name + source_id combination
        if 'source_name' in df.columns and 'source_id' in df.columns:
            combo_df = df[['source_name', 'source_id']].dropna()
            if len(combo_df) > 0:
                unique_combos = combo_df.drop_duplicates()
                duplicate_count = len(combo_df) - len(unique_combos)
                results['unique_source_combo'] = QualityCheckResult(
                    check_name='unique_source_combo',
                    passed=duplicate_count == 0,
                    message=f"Source combinations: {len(unique_combos)} unique, {duplicate_count} duplicates",
                    metric_value=float(duplicate_count)
                )
                self.results.append(results['unique_source_combo'])
        
        # Check that recipes have ingredients
        if 'ingredients' in df.columns:
            recipes_with_ingredients = 0
            total_recipes = len(df)
            isInstanceandLen = False
            
            for _, row in df.iterrows():
                ingredients = normalize_ingredients(row.get("ingredients"))
                if isinstance(ingredients, list) and len(ingredients) > 0:
                    isInstanceandLen = True
                    # Check if any ingredient has a valid name
                    if any(ing.get('ingredient') for ing in ingredients if isinstance(ing, dict)):
                        recipes_with_ingredients += 1
            
            ingredient_coverage = recipes_with_ingredients / total_recipes if total_recipes > 0 else 0
            
            results['recipes_with_ingredients'] = QualityCheckResult(
                check_name='recipes_with_ingredients',
                passed=ingredient_coverage >= 0.8,  # At least 80% of recipes should have ingredients
                message=f"{recipes_with_ingredients}/{total_recipes} recipes have valid ingredients ({ingredient_coverage:.2%}) - total_recipes is {total_recipes} - isInstanceandLen is {isInstanceandLen}",
                metric_value=ingredient_coverage,
                threshold=0.8
            )
            self.results.append(results['recipes_with_ingredients'])
        
        # Check category distribution (warn if too many nulls)
        if 'category' in df.columns:
            results['category_not_null'] = self.check_not_null(df, 'category', threshold=0.5)
        
        return results
    
    def get_summary(self) -> Dict:
        """Get a summary of all quality check results"""
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r.passed)
        failed_checks = total_checks - passed_checks
        
        return {
            'total_checks': total_checks,
            'passed': passed_checks,
            'failed': failed_checks,
            'pass_rate': passed_checks / total_checks if total_checks > 0 else 0.0,
            'timestamp': datetime.utcnow().isoformat(),
            'results': [
                {
                    'check_name': r.check_name,
                    'passed': r.passed,
                    'message': r.message,
                    'metric_value': r.metric_value,
                    'threshold': r.threshold
                }
                for r in self.results
            ]
        }
    
    def log_summary(self):
        """Log a summary of quality check results"""
        summary = self.get_summary()
        logger.info("=" * 60)
        logger.info("DATA QUALITY CHECK SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Checks: {summary['total_checks']}")
        logger.info(f"Passed: {summary['passed']}")
        logger.info(f"Failed: {summary['failed']}")
        logger.info(f"Pass Rate: {summary['pass_rate']:.2%}")
        logger.info("-" * 60)
        
        # Log passed checks first
        passed_results = [r for r in self.results if r.passed]
        if passed_results:
            logger.info("PASSED CHECKS:")
            for result in passed_results:
                logger.info(f"  ✓ {result.check_name}: {result.message}")
        
        # Log failed checks with more detail
        failed_results = [r for r in self.results if not r.passed]
        if failed_results:
            logger.warning("FAILED CHECKS:")
            for result in failed_results:
                logger.warning(f"  ✗ {result.check_name}")
                logger.warning(f"    Message: {result.message}")
                if result.threshold is not None and result.metric_value is not None:
                    logger.warning(f"    Expected: >= {result.threshold:.2%}" if result.threshold < 1 
                                 else f"    Expected: >= {result.threshold}")
                    logger.warning(f"    Actual: {result.metric_value:.2%}" if result.metric_value < 1 
                                 else f"    Actual: {result.metric_value}")
                    if result.metric_value < result.threshold:
                        shortfall = result.threshold - result.metric_value
                        logger.warning(f"    Shortfall: {shortfall:.2%}" if shortfall < 1 else f"    Shortfall: {shortfall}")
                elif result.metric_value is not None:
                    logger.warning(f"    Metric Value: {result.metric_value}")
        
        logger.info("=" * 60)
    
    def get_failed_checks(self) -> List[QualityCheckResult]:
        """Get a list of all failed quality checks"""
        return [r for r in self.results if not r.passed]
    
    def get_failed_checks_details(self) -> List[Dict]:
        """Get detailed information about failed checks"""
        failed = self.get_failed_checks()
        return [
            {
                'check_name': r.check_name,
                'message': r.message,
                'metric_value': r.metric_value,
                'threshold': r.threshold,
                'shortfall': r.threshold - r.metric_value if r.threshold is not None and r.metric_value is not None else None
            }
            for r in failed
        ]
    
    def assert_all_passed(self, raise_on_failure: bool = True):
        """
        Assert that all quality checks passed.
        
        Args:
            raise_on_failure: If True, raise ValueError when checks fail
        
        Raises:
            ValueError: If any checks failed and raise_on_failure is True
        """
        failed_checks = self.get_failed_checks()
        
        if failed_checks and raise_on_failure:
            failed_details = []
            for r in failed_checks:
                detail = f"  - {r.check_name}: {r.message}"
                if r.threshold is not None and r.metric_value is not None:
                    detail += f" (Expected: >= {r.threshold:.2%}, Actual: {r.metric_value:.2%})"
                failed_details.append(detail)
            
            raise ValueError(
                f"Data quality checks failed ({len(failed_checks)} failures):\n" + "\n".join(failed_details)
            )
        
        return len(failed_checks) == 0


