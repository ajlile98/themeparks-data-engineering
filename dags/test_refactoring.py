"""
Test script to validate MinIO setup and DAG refactoring

Run this to ensure:
1. MinIO is accessible
2. Validators module works
3. Pipeline classes can be imported
4. DAG structure is valid
"""

import sys
import os
from datetime import datetime

def test_imports():
    """Test all required imports."""
    print("📦 Testing imports...")
    
    try:
        import pandas as pd
        print("  ✅ pandas")
    except ImportError as e:
        print(f"  ❌ pandas: {e}")
        return False
    
    try:
        import minio
        print("  ✅ minio")
    except ImportError as e:
        print(f"  ❌ minio - Run: uv pip install minio")
        return False
    
    try:
        import pyarrow
        print("  ✅ pyarrow")
    except ImportError as e:
        print(f"  ❌ pyarrow - Run: uv pip install pyarrow")
        return False
    
    try:
        from loaders.MinioStorage import MinioStorage, save_to_minio, load_from_minio
        print("  ✅ MinioStorage")
    except ImportError as e:
        print(f"  ❌ MinioStorage: {e}")
        return False
    
    try:
        from dags import validators
        print("  ✅ validators")
    except ImportError as e:
        print(f"  ❌ validators: {e}")
        return False
    
    try:
        from pipelines import DestinationsPipeline, EntityPipeline, LiveDataPipeline
        print("  ✅ pipeline classes")
    except ImportError as e:
        print(f"  ❌ pipeline classes: {e}")
        return False
    
    return True


def test_minio_connection():
    """Test MinIO connectivity."""
    print("\n🔌 Testing MinIO connection...")
    
    try:
        from loaders.MinioStorage import MinioStorage
        storage = MinioStorage()
        
        # Check if bucket exists
        if storage.client.bucket_exists(storage.bucket):
            print(f"  ✅ Connected to MinIO at {storage.endpoint}")
            print(f"  ✅ Bucket '{storage.bucket}' exists")
        else:
            print(f"  ⚠️  Connected but bucket '{storage.bucket}' was auto-created")
        
        return True
        
    except Exception as e:
        print(f"  ❌ MinIO connection failed: {e}")
        print(f"  💡 Make sure MinIO is running: docker ps | grep minio")
        print(f"  💡 Or run: ./scripts/setup-minio.ps1")
        return False


def test_minio_operations():
    """Test MinIO save/load operations."""
    print("\n💾 Testing MinIO save/load...")
    
    try:
        import pandas as pd
        from loaders.MinioStorage import MinioStorage
        
        storage = MinioStorage()
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Test 1', 'Test 2', 'Test 3'],
            'value': [10, 20, 30]
        })
        
        # Save to MinIO
        metadata = storage.save_dataframe(
            test_df,
            pipeline_name='test_pipeline',
            stage='test'
        )
        print(f"  ✅ Saved test data to MinIO: {metadata['path']}")
        print(f"     Rows: {metadata['row_count']}, Size: {metadata['size_bytes']} bytes")
        
        # Load from MinIO
        loaded_df = storage.load_dataframe(metadata)
        print(f"  ✅ Loaded test data from MinIO: {len(loaded_df)} rows")
        
        # Verify data integrity
        if test_df.equals(loaded_df):
            print(f"  ✅ Data integrity verified")
        else:
            print(f"  ⚠️  Data mismatch after load")
            return False
        
        # Cleanup
        storage.delete_object(metadata)
        print(f"  ✅ Cleaned up test data")
        
        return True
        
    except Exception as e:
        print(f"  ❌ MinIO operations failed: {e}")
        return False


def test_validators():
    """Test validator functions."""
    print("\n✔️  Testing validators...")
    
    try:
        import pandas as pd
        from dags.validators import (
            validate_extract_destinations,
            validate_extract_entities,
            ValidationError
        )
        
        # Test destinations validator with valid data
        valid_df = pd.DataFrame({
            'id': ['dest1', 'dest2'],
            'name': ['Disneyland', 'Universal Studios']
        })
        
        try:
            validate_extract_destinations(valid_df, {'row_count': 2})
            print("  ✅ Destinations validator (valid data)")
        except ValidationError as e:
            print(f"  ❌ Destinations validator failed: {e}")
            return False
        
        # Test entities validator with valid data
        valid_entities = pd.DataFrame({
            'id': ['ent1', 'ent2'],
            'name': ['Space Mountain', 'Splash Mountain'],
            'entityType': ['ATTRACTION', 'ATTRACTION']
        })
        
        try:
            validate_extract_entities(valid_entities, {'row_count': 2})
            print("  ✅ Entities validator (valid data)")
        except ValidationError as e:
            print(f"  ❌ Entities validator failed: {e}")
            return False
        
        # Test validator catches errors (empty data)
        empty_df = pd.DataFrame()
        try:
            validate_extract_destinations(empty_df, {'row_count': 0})
            print("  ❌ Validator should have caught empty data")
            return False
        except ValidationError:
            print("  ✅ Validator catches empty data")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Validator tests failed: {e}")
        return False


def test_dag_import():
    """Test DAG file imports without errors."""
    print("\n🔄 Testing DAG import...")
    
    try:
        # This will import the DAG and execute top-level code
        import dags.themeparks_dag_refactored
        print("  ✅ DAG imports successfully")
        
        # Check DAGs are defined
        if hasattr(dags.themeparks_dag_refactored, 'dag_destinations'):
            print("  ✅ destinations_daily DAG defined")
        
        if hasattr(dags.themeparks_dag_refactored, 'dag_entities'):
            print("  ✅ entities_daily DAG defined")
        
        if hasattr(dags.themeparks_dag_refactored, 'dag_live'):
            print("  ✅ live_data_frequent DAG defined")
        
        if hasattr(dags.themeparks_dag_refactored, 'dag_refresh'):
            print("  ✅ full_refresh_manual DAG defined")
        
        return True
        
    except Exception as e:
        print(f"  ❌ DAG import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 70)
    print("🧪 Theme Parks Pipeline - Refactoring Validation")
    print("=" * 70)
    
    results = []
    
    # Run tests
    results.append(("Imports", test_imports()))
    results.append(("MinIO Connection", test_minio_connection()))
    results.append(("MinIO Operations", test_minio_operations()))
    results.append(("Validators", test_validators()))
    results.append(("DAG Import", test_dag_import()))
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 Test Summary")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print("=" * 70)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Your setup is ready.")
        print("\n📚 Next steps:")
        print("   1. Set environment variables (see dags/REFACTORING_GUIDE.md)")
        print("   2. Deploy DAG to Airflow")
        print("   3. Test run: airflow dags test destinations_daily 2026-02-03")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
