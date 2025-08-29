# utils/supabase_test_runs.py
import os
import requests
from datetime import datetime, timezone
from typing import Tuple, Optional

SUPABASE_TOKEN = os.getenv("SUPABASE_ANON_KEY")
if not SUPABASE_TOKEN:
    raise RuntimeError(
        "Environment variable SUPABASE_ANON_KEY is missing. "
        "Set it before running tests."
    )

SUPABASE_FUNC_CREATE_URL = os.getenv(
    "SUPABASE_FUNC_CREATE_URL",
    "https://whnvlzdyeertvilbymng.supabase.co/functions/v1/test-runs/create",
)
SUPABASE_FUNC_COMPLETE_URL = os.getenv(
    "SUPABASE_FUNC_COMPLETE_URL",
    "https://whnvlzdyeertvilbymng.supabase.co/functions/v1/test-runs/complete",
)


DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {SUPABASE_TOKEN}",
}

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

TEST_TYPES = {
    "e2e":   "08a274a3-4cd2-43ca-8b4d-072dc5fae360",  # E2E Tests
    "stress":"4b342177-9307-499b-a8a8-5a56e33e944e",  # Full Stress Test
    "e2e_k8s": "2400cea9-4859-4329-b383-03e8705f1f3d", # E2E Test - K8s
    "upgrade": "cfa01529-fcc8-44ef-aa12-8fb7cb16bc32", # Upgrade Tests
}

ENVIRONMENTS_BY_SUFFIX = {
    ".210": "c5981249-58b9-428e-aa94-0556882219df",  # lab-bare-metal-1
    ".211": "aeda1aef-6ea3-49d0-87dd-ec259ae9c507",  # lab-bare-metal-2
    ".141": "e7b813fc-a850-40cd-bccd-e2032919ce42",  # lab-small-02
    ".111": "12b89016-9241-4fe0-9a9e-8f707e1b7da7",  # lab-small-03
    ".61":  "08a5e01a-f964-40bb-87c3-184b02c2c4a6",  # lab-small-06
    ".66":  "7cf92329-67fd-489c-875d-4a2f9d7959e1",  # lab-small-07
    ".81":  "519cab3a-1f9f-421a-a99e-2436a8a82dd4",  # lab-small-08
    ".146": "36da12fe-1710-42ff-b457-1b525d296504",  # lab-small-09
    # "AWS": "57ac99d9-cab7-4f7c-b841-b88f09ab0d43",
    # "GCP": "b2801ab7-b1bd-48d2-84a1-54580fbef478",
}

FAILURE_REASON_OTHER = "01d766fa-78d5-4215-8ffb-2e99b4877504"


def resolve_environment_id_from_ip(mgmt_ip: str) -> Optional[str]:
    """
    Given a management node IP like '10.10.10.81', derive '.81' and map to environment_id.
    """
    try:
        last_octet = mgmt_ip.strip().split(".")[-1]
        suffix = f".{last_octet}"
        return ENVIRONMENTS_BY_SUFFIX.get(suffix)
    except Exception:
        return None

def detect_fe_be_tags(ssh_obj, client_ip: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    cmd = "docker images --digests --format '{{.Repository}} {{.Tag}} {{.Digest}}'"
    out, _ = ssh_obj.exec_command(client_ip, cmd)

    fe_branch = fe_commit = be_branch = be_commit = None
    for raw in out.splitlines():
        parts = raw.split()
        if len(parts) < 3:
            continue
        repo, tag, digest = parts[0], parts[1], parts[2]
        digest = (digest or "").replace("sha256:", "").strip()
        if "/" in repo:
            repo = repo.split("/")[-1]

        if "simplyblock" in repo:  # FE
            fe_branch = tag
            fe_commit = digest
        elif "spdk" in repo or "ultra" in repo:       # BE
            be_branch = tag
            be_commit = digest

    return fe_branch, fe_commit, be_branch, be_commit


class TestRunsAPI:
    def __init__(self, test_type_key: str):
        if test_type_key not in TEST_TYPES:
            raise ValueError(f"Unknown test_type '{test_type_key}'. Valid: {list(TEST_TYPES.keys())}")
        self.test_type_id = TEST_TYPES[test_type_key]
        self.run_id: Optional[str] = None

    def create_run(
        self,
        jira_ticket: str,
        environment_id: str,  # now required (we’ll resolve from mgmt IP in caller)
        github_branch_frontend: Optional[str] = None,
        github_branch_backend: Optional[str] = None,
        github_commit_tag_frontend: Optional[str] = None,
        github_commit_tag_backend: Optional[str] = None,
    ) -> str:
        payload = {
            "test_type_id": self.test_type_id,
            "environment_id": environment_id,
            "jira_ticket": jira_ticket,  # can be ""
            "github_branch_frontend": github_branch_frontend,
            "github_branch_backend": github_branch_backend,
            "github_commit_tag_frontend": github_commit_tag_frontend,
            "github_commit_tag_backend": github_commit_tag_backend,
        }

        resp = requests.post(SUPABASE_FUNC_CREATE_URL, headers=DEFAULT_HEADERS, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        new_id = data.get("id") or data.get("test_run_id") or data.get("data", {}).get("id")
        if not new_id:
            raise RuntimeError(f"Create run response missing 'id': {data}")
        self.run_id = str(new_id)
        return self.run_id

    def complete_run(
        self,
        status: str,  # "completed" | "failed"
        completion_comment: str,
        completion_jira_ticket: Optional[str] = None,
        failure_reason_id: Optional[str] = None,
        errors: dict = None,
    ):
        if not self.run_id:
            raise RuntimeError("No run_id found. Call create_run() first.")
        
        comment = self._format_comment(completion_comment, errors)

        body = {
            "test_run_id": self.run_id,
            "status": status,
            "completion_comment": comment,
            "completion_jira_ticket": completion_jira_ticket,
        }
        if status == "failed" and failure_reason_id:
            body["failure_reason_id"] = failure_reason_id

        resp = requests.put(SUPABASE_FUNC_COMPLETE_URL, headers=DEFAULT_HEADERS, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _format_comment(summary: str, errors: dict | None) -> str:
        """
        Take summary (raw test results string) + errors dict and
        return a nicely formatted message.
        """
        if not errors:
            return summary

        lines = [summary, "\n---\n🚨 *Error Details:*"]
        for test_name, err_list in errors.items():
            for e in err_list:
                etype = type(e).__name__
                msg = str(e) or repr(e)
                # truncate if very long
                if len(msg) > 300:
                    msg = msg[:299] + "…"
                lines.append(f"- `{test_name}` → {etype}: {msg}")
        return "\n".join(lines)
