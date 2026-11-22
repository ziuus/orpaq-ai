import asyncio
import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.services.audit_queue import AsyncVisionWorkerQueue, audit_queue


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


class TestAsyncVisionWorkerQueue:
    def test_bounded_concurrency(self):
        """Verify that concurrent execution never exceeds max_concurrency (3)."""
        active_count = 0
        peak_concurrency = 0

        async def mock_analysis(url: str, screenshot_only: bool = False):
            nonlocal active_count, peak_concurrency
            active_count += 1
            if active_count > peak_concurrency:
                peak_concurrency = active_count
            await asyncio.sleep(0.05)
            active_count -= 1
            return {"pagespeed_score": 90, "ai_ui_score": 8.5}

        async def run_test():
            queue = AsyncVisionWorkerQueue(max_concurrency=3, analysis_fn=mock_analysis)
            urls = [f"https://example{i}.com" for i in range(8)]
            batch_id = queue.enqueue(urls)

            # Wait for batch completion
            task = queue._batch_tasks[batch_id]
            await task

            status = queue.get_batch_status(batch_id)
            assert status["status"] == "completed"
            assert status["completed_count"] == 8
            assert status["failed_count"] == 0
            assert peak_concurrency <= 3

        asyncio.run(run_test())

    def test_per_url_error_isolation(self):
        """Verify that a failing URL does not abort the batch or affect other URLs."""
        async def mock_analysis(url: str, screenshot_only: bool = False):
            if "fail" in url:
                raise ConnectionError("DNS resolution failed for domain")
            return {"pagespeed_score": 80, "ai_ui_score": 7.0}

        async def run_test():
            queue = AsyncVisionWorkerQueue(max_concurrency=3, analysis_fn=mock_analysis)
            urls = [
                "https://good1.com",
                "https://fail.com",
                "https://good2.com",
            ]
            batch_id = queue.enqueue(urls)
            await queue._batch_tasks[batch_id]

            status = queue.get_batch_status(batch_id)
            assert status["status"] == "completed"
            assert status["total_urls"] == 3
            assert status["completed_count"] == 2
            assert status["failed_count"] == 1

            # Check individual results
            results_by_url = {r["url"]: r for r in status["results"]}
            assert results_by_url["https://good1.com"]["status"] == "completed"
            assert results_by_url["https://good1.com"]["error"] is None
            assert results_by_url["https://fail.com"]["status"] == "failed"
            assert "DNS resolution failed" in results_by_url["https://fail.com"]["error"]
            assert results_by_url["https://good2.com"]["status"] == "completed"

        asyncio.run(run_test())

    def test_batch_cancellation(self):
        """Verify that canceling a batch aborts pending items cleanly."""
        async def slow_analysis(url: str, screenshot_only: bool = False):
            await asyncio.sleep(1.0)
            return {"pagespeed_score": 100}

        async def run_test():
            queue = AsyncVisionWorkerQueue(max_concurrency=1, analysis_fn=slow_analysis)
            batch_id = queue.enqueue(["https://slow1.com", "https://slow2.com"])
            await asyncio.sleep(0.05)  # Let first item start
            cancelled = queue.cancel_batch(batch_id)
            assert cancelled is True

            status = queue.get_batch_status(batch_id)
            assert status["status"] == "cancelled"

        asyncio.run(run_test())

    def test_empty_urls_rejected(self):
        queue = AsyncVisionWorkerQueue(max_concurrency=3)
        with pytest.raises(ValueError, match="URLs list cannot be empty"):
            queue.enqueue([])

    def test_queue_shutdown(self):
        async def hanging_analysis(url: str, screenshot_only: bool = False):
            await asyncio.sleep(10.0)
            return {}

        async def run_test():
            queue = AsyncVisionWorkerQueue(max_concurrency=2, analysis_fn=hanging_analysis)
            queue.enqueue(["https://hang1.com", "https://hang2.com"])
            await asyncio.sleep(0.05)
            await queue.shutdown()
            assert queue._is_shutting_down is True

        asyncio.run(run_test())


class TestBatchAuditRoutes:
    def test_post_batch_audit_success(self, client):
        payload = {"urls": ["https://site1.com", "https://site2.com"], "screenshot_only": True}
        response = client.post("/api/v1/audit/batch", json=payload)
        assert response.status_code == 202
        data = response.json()
        assert "batch_id" in data
        assert data["status"] in ("pending", "processing", "completed")
        assert data["total_urls"] == 2

    def test_post_batch_audit_empty_urls_rejected(self, client):
        response = client.post("/api/v1/audit/batch", json={"urls": []})
        assert response.status_code in (400, 422)

    def test_get_batch_audit_status_flow(self, client):
        post_resp = client.post("/api/v1/audit/batch", json={"urls": ["https://testsite.com"]})
        assert post_resp.status_code == 202
        batch_id = post_resp.json()["batch_id"]

        get_resp = client.get(f"/api/v1/audit/batch/{batch_id}")
        assert get_resp.status_code == 200
        data = get_resp.json()
        assert data["batch_id"] == batch_id
        assert data["total_urls"] == 1
        assert "results" in data
        assert len(data["results"]) == 1

    def test_get_nonexistent_batch_returns_404(self, client):
        response = client.get("/api/v1/audit/batch/nonexistent-id-12345")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_cancel_batch_endpoint(self, client):
        post_resp = client.post(
            "/api/v1/audit/batch",
            json={"urls": ["https://site-to-cancel.com"]}
        )
        batch_id = post_resp.json()["batch_id"]

        cancel_resp = client.post(f"/api/v1/audit/batch/{batch_id}/cancel")
        assert cancel_resp.status_code == 200
        data = cancel_resp.json()
        assert data["batch_id"] == batch_id
        assert data["status"] == "cancelled"

    def test_cancel_nonexistent_batch_returns_404(self, client):
        response = client.post("/api/v1/audit/batch/nonexistent-batch-id/cancel")
        assert response.status_code == 404
