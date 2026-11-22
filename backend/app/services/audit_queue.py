import asyncio
import hashlib
import uuid
from datetime import datetime, timezone
from typing import Callable, Optional
from app.services.website_analysis import website_analysis


class AsyncVisionWorkerQueue:
    def __init__(
        self,
        max_concurrency: int = 3,
        analysis_fn: Optional[Callable] = None,
    ):
        self.max_concurrency = max_concurrency
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._batches: dict[str, dict] = {}
        self._batch_tasks: dict[str, asyncio.Task] = {}
        self._is_shutting_down: bool = False
        self._analysis_fn = analysis_fn

    @property
    def semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrency)
        return self._semaphore

    def enqueue(self, urls: list[str], screenshot_only: bool = False) -> str:
        if not urls:
            raise ValueError("URLs list cannot be empty")

        batch_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        items = [
            {
                "url": url,
                "status": "pending",
                "pagespeed_score": None,
                "pagespeed_issues": None,
                "mobile_friendly": None,
                "ai_ui_score": None,
                "ai_outdated": None,
                "ai_issues": None,
                "ai_suggestions": None,
                "screenshot_url": None,
                "error": None,
                "completed_at": None,
            }
            for url in urls
        ]

        self._batches[batch_id] = {
            "batch_id": batch_id,
            "status": "pending",
            "total_urls": len(urls),
            "completed_count": 0,
            "failed_count": 0,
            "created_at": now,
            "updated_at": now,
            "results": items,
        }

        task = asyncio.create_task(
            self._process_batch(batch_id, screenshot_only=screenshot_only)
        )
        self._batch_tasks[batch_id] = task
        return batch_id

    async def _process_batch(self, batch_id: str, screenshot_only: bool = False):
        batch = self._batches[batch_id]
        if batch["status"] == "cancelled":
            return
        batch["status"] = "processing"
        batch["updated_at"] = datetime.now(timezone.utc).isoformat()

        async def _process_item(item: dict):
            async with self.semaphore:
                if self._is_shutting_down:
                    item["status"] = "failed"
                    item["error"] = "Queue is shutting down"
                    batch["failed_count"] += 1
                    item["completed_at"] = datetime.now(timezone.utc).isoformat()
                    return

                if batch["status"] == "cancelled":
                    item["status"] = "failed"
                    item["error"] = "Batch cancelled"
                    item["completed_at"] = datetime.now(timezone.utc).isoformat()
                    return

                item["status"] = "processing"
                try:
                    res = await self._execute_analysis(
                        item["url"], screenshot_only=screenshot_only
                    )
                    item["pagespeed_score"] = res.get("pagespeed_score")
                    item["pagespeed_issues"] = res.get("pagespeed_issues")
                    item["mobile_friendly"] = res.get("mobile_friendly")
                    item["ai_ui_score"] = res.get("ai_ui_score")
                    item["ai_outdated"] = res.get("ai_outdated")
                    item["ai_issues"] = res.get("ai_issues")
                    item["ai_suggestions"] = res.get("ai_suggestions")
                    item["screenshot_url"] = res.get("screenshot_url")
                    item["status"] = "completed"
                    batch["completed_count"] += 1
                except asyncio.CancelledError:
                    item["status"] = "failed"
                    item["error"] = "Task cancelled"
                    batch["failed_count"] += 1
                    raise
                except Exception as exc:
                    # Per-URL error isolation: do not abort remaining items
                    item["status"] = "failed"
                    item["error"] = str(exc)
                    batch["failed_count"] += 1
                finally:
                    item["completed_at"] = datetime.now(timezone.utc).isoformat()
                    batch["updated_at"] = datetime.now(timezone.utc).isoformat()

        tasks = [asyncio.create_task(_process_item(item)) for item in batch["results"]]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            batch["status"] = "cancelled"
            batch["updated_at"] = datetime.now(timezone.utc).isoformat()
            return

        if batch["status"] != "cancelled":
            batch["status"] = "completed"
        batch["updated_at"] = datetime.now(timezone.utc).isoformat()

    async def _execute_analysis(self, url: str, screenshot_only: bool = False) -> dict:
        if self._analysis_fn:
            return await self._analysis_fn(url, screenshot_only)
        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        item_id = f"batch_{url_hash}"
        return await website_analysis.analyze_website(
            business_id=item_id,
            website_url=url,
            screenshot_only=screenshot_only,
        )

    def get_batch_status(self, batch_id: str) -> Optional[dict]:
        return self._batches.get(batch_id)

    def cancel_batch(self, batch_id: str) -> bool:
        if batch_id not in self._batches:
            return False
        batch = self._batches[batch_id]
        batch["status"] = "cancelled"
        batch["updated_at"] = datetime.now(timezone.utc).isoformat()
        for item in batch["results"]:
            if item["status"] in ("pending", "processing"):
                item["status"] = "failed"
                item["error"] = "Batch cancelled"
                item["completed_at"] = datetime.now(timezone.utc).isoformat()

        task = self._batch_tasks.get(batch_id)
        if task and not task.done():
            task.cancel()
        return True

    async def shutdown(self):
        self._is_shutting_down = True
        for task in self._batch_tasks.values():
            if not task.done():
                task.cancel()
        tasks = [t for t in self._batch_tasks.values() if not t.done()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await website_analysis.close_browser()
        except Exception:
            pass


audit_queue = AsyncVisionWorkerQueue()
