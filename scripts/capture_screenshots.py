"""Capture the four proof screenshots for docs/screenshots/.

Renders the real Trino CLI output to an image and drives the live MinIO,
Airflow, and Streamlit UIs with Playwright (system Chrome). Each target is
independent so one failure does not block the rest. Re-runnable while the
stack is up:

    pip install playwright
    python scripts/capture_screenshots.py
"""
import html
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT_DIR = Path(__file__).resolve().parent.parent / "docs" / "screenshots"
# Kept next to this script so the path resolves identically under Git Bash and
# Windows Python (a literal /tmp would point at C:\tmp under Windows Python).
TRINO_SESSION = Path(__file__).resolve().parent / "_trino_session.txt"

AIRFLOW_USER = "admin"
AIRFLOW_PASSWORD = sys.argv[1] if len(sys.argv) > 1 else "admin"
MINIO_USER = "minioadmin"
MINIO_PASSWORD = "minioadmin"

results = {}


def _first(page, selectors, value):
    for sel in selectors:
        loc = page.locator(sel)
        try:
            if loc.count() > 0:
                loc.first.fill(value, timeout=4000)
                return True
        except Exception:
            continue
    return False


def capture_trino(context):
    text = TRINO_SESSION.read_text() if TRINO_SESSION.exists() else "(no trino output captured)"
    page = context.new_page()
    doc = f"""
    <html><body style="margin:0;background:#0d1117;">
      <div style="padding:22px;">
        <div style="color:#9cdcfe;font:600 15px/1.4 'Segoe UI',sans-serif;margin-bottom:10px;">
          Trino query results &mdash; iceberg.quality tables
        </div>
        <pre style="background:#161b22;color:#d1d5da;padding:18px 20px;border-radius:8px;
             font:13px/1.5 'Cascadia Code','Consolas',monospace;white-space:pre;
             border:1px solid #30363d;display:inline-block;">{html.escape(text)}</pre>
      </div>
    </body></html>"""
    page.set_content(doc)
    page.wait_for_timeout(400)
    page.locator("body").screenshot(path=str(OUT_DIR / "trino-query.png"))
    page.close()
    results["trino-query.png"] = "ok"


def capture_streamlit(context):
    page = context.new_page()
    page.goto("http://localhost:8501", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_selector("text=Lakehouse Transactions Dashboard", timeout=30000)
    page.wait_for_timeout(3500)  # let charts render
    page.screenshot(path=str(OUT_DIR / "streamlit-dashboard.png"), full_page=True)
    page.close()
    results["streamlit-dashboard.png"] = "ok"


def _dismiss_modals(page):
    # MinIO shows a license/EULA modal on first load that overlays the browser.
    for sel in ["button:has-text('Acknowledge')", "button:has-text('Accept')", "button:has-text('I Agree')"]:
        try:
            if page.locator(sel).count() > 0:
                page.locator(sel).first.click(timeout=3000)
                page.wait_for_timeout(800)
        except Exception:
            pass


def capture_minio(context):
    page = context.new_page()
    page.goto("http://localhost:9001", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(1500)
    _first(page, ["#accessKey", "input[name='accessKey']", "input[placeholder='Username']"], MINIO_USER)
    _first(
        page,
        ["#secretKey", "input[name='secretKey']", "input[placeholder='Password']", "input[type='password']"],
        MINIO_PASSWORD,
    )
    for sel in ["button[type='submit']", "button:has-text('Login')", "button:has-text('Log In')"]:
        if page.locator(sel).count() > 0:
            page.locator(sel).first.click()
            break
    page.wait_for_timeout(2500)
    _dismiss_modals(page)
    # Browse into the warehouse bucket's quality prefix to show the Iceberg data.
    page.goto("http://localhost:9001/browser/warehouse/quality%2F", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(2500)
    _dismiss_modals(page)
    page.wait_for_timeout(1500)
    page.screenshot(path=str(OUT_DIR / "minio-bucket.png"), full_page=True)
    page.close()
    results["minio-bucket.png"] = "ok"


def capture_airflow(context):
    page = context.new_page()
    page.goto("http://localhost:8088/login/", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(1500)
    _first(page, ["#username", "input[name='username']"], AIRFLOW_USER)
    _first(page, ["#password", "input[name='password']"], AIRFLOW_PASSWORD)
    for sel in ["button[type='submit']", "input[type='submit']", "button:has-text('Sign In')"]:
        if page.locator(sel).count() > 0:
            page.locator(sel).first.click()
            break
    page.wait_for_timeout(2500)
    page.goto("http://localhost:8088/dags/transactions_backfill/grid", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(3500)
    page.screenshot(path=str(OUT_DIR / "airflow-dag.png"), full_page=True)
    page.close()
    results["airflow-dag.png"] = "ok"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        context = browser.new_context(viewport={"width": 1366, "height": 900})
        for name, fn in [
            ("trino", capture_trino),
            ("streamlit", capture_streamlit),
            ("minio", capture_minio),
            ("airflow", capture_airflow),
        ]:
            try:
                fn(context)
            except Exception as exc:  # noqa: BLE001 - report and continue
                results[name] = f"FAILED: {exc}"
        browser.close()
    for k, v in results.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
