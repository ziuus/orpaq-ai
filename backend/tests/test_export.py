import csv
import io
import json
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.database import db


@pytest.fixture(autouse=True)
def setup_test_db(tmp_path, monkeypatch):
    test_db_path = tmp_path / "test_leads.db"
    monkeypatch.setattr("app.services.database.DB_PATH", test_db_path)
    db.init()
    yield
    db.close()
    if test_db_path.exists():
        test_db_path.unlink()


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture
def seed_test_data():
    b1 = db.create_business(
        {
            "name": "Acme Dental",
            "website": "https://acmedental.example",
            "phone": "+1-555-0199",
            "email": "contact@acmedental.example",
            "category": "dentists",
            "location": "New York, NY",
            "address": "123 Broadway",
            "rating": 4.8,
            "reviews_count": 120,
            "source": "google_maps",
        }
    )
    a1 = db.create_analysis(
        {
            "business_id": b1["id"],
            "pagespeed_score": 45,
            "pagespeed_issues": ["Slow LCP (4.2s)", "Unoptimized hero images"],
            "mobile_friendly": True,
            "ai_ui_score": 6.5,
            "ai_outdated": False,
            "ai_issues": ["Contrast ratio too low", "Missing clear booking CTA"],
            "ai_suggestions": "Add an online booking button in the hero section.",
            "analysis_status": "completed",
        }
    )
    l1 = db.create_lead(
        {
            "business_id": b1["id"],
            "lead_score": 85,
            "priority": "hot",
            "outreach_message": 'Hello Acme,\nWe noticed your booking page has a slow load time.\n"Improving it can boost conversions by 30%."',
            "redesign_suggestions": "Redesign mobile navbar, add sticky CTA button.",
        }
    )

    b2 = db.create_business(
        {
            "name": "Rustic Salon",
            "website": None,
            "phone": "+1-555-0200",
            "email": None,
            "category": "salons",
            "location": "Chicago, IL",
            "address": "456 State St",
            "rating": 3.9,
            "reviews_count": 15,
            "source": "google_maps",
        }
    )
    l2 = db.create_lead(
        {
            "business_id": b2["id"],
            "lead_score": 40,
            "priority": "low",
            "outreach_message": "Hello Rustic Salon,\nYou do not have a website.",
            "redesign_suggestions": "Create a modern 1-page salon site.",
        }
    )
    return {"b1": b1, "a1": a1, "l1": l1, "b2": b2, "l2": l2}


def test_db_get_leads_includes_gap_and_metrics(seed_test_data):
    leads = db.get_leads(limit=10)
    assert len(leads) == 2
    lead1 = next(l for l in leads if l["businesses"]["name"] == "Acme Dental")

    # Assert gap analysis fields exist and are populated
    analysis = lead1["analyses"]
    assert analysis["pagespeed_score"] == 45
    assert analysis["pagespeed_issues"] == ["Slow LCP (4.2s)", "Unoptimized hero images"]
    assert analysis["mobile_friendly"] is True
    assert analysis["ai_ui_score"] == 6.5
    assert analysis["ai_outdated"] is False
    assert analysis["ai_issues"] == ["Contrast ratio too low", "Missing clear booking CTA"]
    assert "booking button" in analysis["ai_suggestions"]

    # Assert lead and business metrics exist
    biz = lead1["businesses"]
    assert biz["email"] == "contact@acmedental.example"
    assert biz["address"] == "123 Broadway"
    assert biz["rating"] == 4.8
    assert biz["reviews_count"] == 120
    assert biz["source"] == "google_maps"
    assert "slow load time" in lead1["outreach_message"]
    assert "sticky CTA" in lead1["redesign_suggestions"]


def test_db_get_leads_multi_analysis_deduplication():
    b = db.create_business(
        {
            "name": "Unique Tech",
            "website": "https://uniquetech.example",
            "category": "tech",
            "location": "Austin, TX",
        }
    )
    # Older analysis
    a_old = db.create_analysis(
        {
            "business_id": b["id"],
            "pagespeed_score": 30,
            "analysis_status": "completed",
        }
    )
    # Newer analysis
    a_new = db.create_analysis(
        {
            "business_id": b["id"],
            "pagespeed_score": 95,
            "analysis_status": "completed",
        }
    )
    db.create_lead(
        {
            "business_id": b["id"],
            "lead_score": 90,
            "priority": "hot",
        }
    )

    leads = db.get_leads(limit=10)
    assert len(leads) == 1
    assert leads[0]["analyses"]["id"] == a_new["id"]
    assert leads[0]["analyses"]["pagespeed_score"] == 95


def test_export_csv_streaming_and_headers(client, seed_test_data):
    response = client.post("/api/v1/export", json={"format": "csv", "direct_download": True})
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert 'attachment; filename="leads_export.csv"' in response.headers["content-disposition"]

    csv_reader = csv.reader(io.StringIO(response.text))
    rows = list(csv_reader)
    header = rows[0]

    expected_headers = [
        "Business Name",
        "Website",
        "Phone",
        "Email",
        "Category",
        "Location",
        "Address",
        "Rating",
        "Reviews Count",
        "Source",
        "Lead Score",
        "Priority",
        "Outreach Message",
        "Redesign Suggestions",
        "PageSpeed Score",
        "PageSpeed Issues",
        "Mobile Friendly",
        "AI UI Score",
        "Outdated",
        "AI Issues",
        "AI Suggestions",
    ]
    assert header == expected_headers
    assert len(rows) == 3  # 1 header + 2 data rows
    for r in rows:
        assert len(r) == len(expected_headers)


def test_export_csv_escaping_multiline_and_json(client, seed_test_data):
    response = client.post("/api/v1/export", json={"format": "csv", "direct_download": True})
    csv_reader = csv.reader(io.StringIO(response.text))
    rows = list(csv_reader)

    acme_row = next(r for r in rows if r[0] == "Acme Dental")
    outreach_col = rows[0].index("Outreach Message")
    ps_issues_col = rows[0].index("PageSpeed Issues")
    ai_issues_col = rows[0].index("AI Issues")
    mobile_col = rows[0].index("Mobile Friendly")

    # Multi-line with embedded quotes must round-trip cleanly without splitting rows
    assert "\n" in acme_row[outreach_col]
    assert '"Improving it can boost conversions by 30%."' in acme_row[outreach_col]

    # JSON arrays must be valid JSON strings
    ps_issues = json.loads(acme_row[ps_issues_col])
    assert isinstance(ps_issues, list)
    assert "Slow LCP (4.2s)" in ps_issues

    ai_issues = json.loads(acme_row[ai_issues_col])
    assert isinstance(ai_issues, list)
    assert "Contrast ratio too low" in ai_issues

    assert acme_row[mobile_col] == "Yes"


def test_export_csv_filter_propagation(client, seed_test_data):
    # Filter by category
    resp_category = client.post(
        "/api/v1/export",
        json={"format": "csv", "filters": {"category": "dentists"}, "direct_download": True},
    )
    rows = list(csv.reader(io.StringIO(resp_category.text)))
    assert len(rows) == 2  # header + 1 matching row
    assert rows[1][0] == "Acme Dental"

    # Filter by website existence
    resp_no_web = client.post(
        "/api/v1/export",
        json={"format": "csv", "filters": {"has_website": False}, "direct_download": True},
    )
    rows_no_web = list(csv.reader(io.StringIO(resp_no_web.text)))
    assert len(rows_no_web) == 2
    assert rows_no_web[1][0] == "Rustic Salon"

    # Filter by priority
    resp_priority = client.post(
        "/api/v1/export",
        json={"format": "csv", "filters": {"priority": "hot"}, "direct_download": True},
    )
    rows_priority = list(csv.reader(io.StringIO(resp_priority.text)))
    assert len(rows_priority) == 2
    assert rows_priority[1][0] == "Acme Dental"

    # Filter by search
    resp_search = client.post(
        "/api/v1/export",
        json={"format": "csv", "filters": {"search": "Rustic"}, "direct_download": True},
    )
    rows_search = list(csv.reader(io.StringIO(resp_search.text)))
    assert len(rows_search) == 2
    assert rows_search[1][0] == "Rustic Salon"


def test_export_get_csv_endpoint(client, seed_test_data):
    response = client.get("/api/v1/export/csv?category=dentists&priority=hot")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert 'attachment; filename="leads_export.csv"' in response.headers["content-disposition"]
    rows = list(csv.reader(io.StringIO(response.text)))
    assert len(rows) == 2
    assert rows[1][0] == "Acme Dental"


def test_export_json_format_backward_compat(client, seed_test_data):
    response = client.post("/api/v1/export", json={"format": "json"})
    assert response.status_code == 200
    data = response.json()
    assert "leads" in data
    assert len(data["leads"]) == 2


def test_export_direct_download_false_backward_compat(client, seed_test_data):
    response = client.post(
        "/api/v1/export", json={"format": "csv", "direct_download": False}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["format"] == "csv"
    assert "data" in data
    assert "Business Name,Website,Phone" in data["data"]
