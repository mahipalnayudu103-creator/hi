import os
import sys
import unittest
from pathlib import Path

# Add backend directory to sys.path
backend_dir = Path(__file__).resolve().parent.parent / "backend"
sys.path.insert(0, str(backend_dir))

class TestSmoke(unittest.TestCase):
    def test_imports(self):
        """Verify that backend modules can be imported without errors."""
        try:
            import app
            import config
            from routes.api import router
            from services.pipeline import run_build_pipeline
            from utils.csv_reader import resolve_csv_path
            self.assertIsNotNone(app.app)
            self.assertIsNotNone(router)
        except Exception as e:
            self.fail(f"Failed to import backend modules: {e}")

    def test_fastapi_instance(self):
        """Verify that the FastAPI app instance is configured."""
        from fastapi.testclient import TestClient
        from app import app
        
        client = TestClient(app)
        response = client.get("/api/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

if __name__ == "__main__":
    unittest.main()
