from datetime import datetime
from typing import Optional, Any, Union
from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class BusinessBase(BaseModel):
    name: str
    website: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    category: str
    location: str
    address: Optional[str] = None
    rating: Optional[float] = None
    reviews_count: Optional[int] = None
    source: str = "google_maps"


class BusinessCreate(BusinessBase):
    pass


class Business(BusinessBase):
    model_config = ConfigDict(from_attributes=True)

    id: str
    created_at: datetime
    updated_at: datetime


class WebsiteAnalysisBase(BaseModel):
    business_id: str
    screenshot_url: Optional[str] = None
    pagespeed_score: Optional[int] = None
    pagespeed_issues: Optional[Union[dict, list[Any], str]] = None
    mobile_friendly: Optional[bool] = None
    ai_ui_score: Optional[float] = None
    ai_outdated: Optional[bool] = None
    ai_issues: Optional[list[str]] = None
    ai_suggestions: Optional[str] = None
    analysis_status: str = "pending"


class WebsiteAnalysisCreate(WebsiteAnalysisBase):
    pass


class WebsiteAnalysis(WebsiteAnalysisBase):
    model_config = ConfigDict(from_attributes=True)

    id: str
    created_at: datetime
    updated_at: datetime


class LeadBase(BaseModel):
    business_id: str
    lead_score: int
    priority: str = "low"
    outreach_message: Optional[str] = None
    redesign_suggestions: Optional[str] = None


class LeadCreate(LeadBase):
    pass


class Lead(LeadBase):
    model_config = ConfigDict(from_attributes=True)

    id: str
    created_at: datetime
    updated_at: datetime


class LeadWithBusiness(Lead):
    business: Optional[Business] = None
    analysis: Optional[WebsiteAnalysis] = None


class PipelineRun(BaseModel):
    location: str
    category: str
    status: str = "running"
    businesses_found: int = 0
    businesses_analyzed: int = 0
    leads_generated: int = 0


class FetchBusinessesRequest(BaseModel):
    location: str
    category: str
    limit: int = Field(default=50, le=200)
    source: str = "mock"


class AnalyzeWebsiteRequest(BaseModel):
    business_id: str
    screenshot_only: bool = False


class RunPipelineRequest(BaseModel):
    location: str
    category: str
    limit: int = Field(default=50, le=200)


class LeadFilters(BaseModel):
    priority: Optional[str] = None
    min_score: Optional[int] = None
    max_score: Optional[int] = None
    has_website: Optional[bool] = None
    location: Optional[str] = None
    category: Optional[str] = None
    search: Optional[str] = None


class ExportRequest(BaseModel):
    format: str = "csv"
    filters: Optional[LeadFilters] = None
    direct_download: Optional[bool] = True
    limit: Optional[int] = 50000


class BatchAuditRequest(BaseModel):
    urls: list[str] = Field(..., min_length=1, description="List of website domain URLs to audit in batch")
    screenshot_only: bool = Field(default=False, description="If True, only capture website screenshots without full PageSpeed/AI Vision")


class BatchAuditItemResult(BaseModel):
    url: str
    status: str = Field(default="pending", description="'pending' | 'processing' | 'completed' | 'failed'")
    pagespeed_score: Optional[int] = None
    pagespeed_issues: Optional[Union[dict, list[Any], str]] = None
    mobile_friendly: Optional[bool] = None
    ai_ui_score: Optional[float] = None
    ai_outdated: Optional[bool] = None
    ai_issues: Optional[list[str]] = None
    ai_suggestions: Optional[str] = None
    screenshot_url: Optional[str] = None
    error: Optional[str] = None
    completed_at: Optional[str] = None


class BatchAuditResponse(BaseModel):
    batch_id: str
    status: str = "pending"
    total_urls: int
    created_at: str
    message: str = "Batch audit queued successfully"


class BatchAuditStatusResponse(BaseModel):
    batch_id: str
    status: str = Field(..., description="Batch lifecycle status: 'pending' | 'processing' | 'completed' | 'failed' | 'cancelled'")
    total_urls: int
    completed_count: int = 0
    failed_count: int = 0
    created_at: str
    updated_at: str
    results: list[BatchAuditItemResult] = []


class PipelineRequestCreate(BaseModel):
    location: str
    category: str
    limit_count: int = 20


class DeleteLeadsByFilterRequest(BaseModel):
    priority: Optional[str] = None
    location: Optional[str] = None
    category: Optional[str] = None
