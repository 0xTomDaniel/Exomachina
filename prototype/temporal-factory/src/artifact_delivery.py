"""Authenticated delivery helpers for accepted verified-research artifacts.

The pinned Quality digest covers the serialized report envelope. The report's
Markdown is a field inside that envelope, so this module returns the exact
Markdown bytes and reports both digests without conflating them.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping


class ArtifactNotFound(LookupError):
    pass


@dataclass(frozen=True)
class DeliveredMarkdown:
    content: bytes
    markdown_sha256: str
    accepted_sha256: str
    accepted_content_sha256: str
    revision: str
    media_type: str = "text/markdown; charset=utf-8"


def accepted_markdown(artifact: Mapping[str, Any], *, revision: str,
                      sha256: str) -> DeliveredMarkdown:
    """Validate the accepted envelope, then return its exact Markdown bytes.

    `sha256` is the accepted artifact identity. Since that digest covers the
    report envelope, `markdown_sha256` is also returned for byte-level browser
    verification of the extracted Markdown.
    """
    if not isinstance(artifact, Mapping) or artifact.get("revision") != revision:
        raise ArtifactNotFound("accepted artifact not found")
    accepted_digest = artifact.get("sha256")
    content = artifact.get("content")
    if (not isinstance(accepted_digest, str) or accepted_digest != sha256
            or not isinstance(content, str)):
        raise ArtifactNotFound("accepted artifact not found")
    accepted_bytes = content.encode("utf-8")
    actual_accepted_digest = hashlib.sha256(accepted_bytes).hexdigest()
    if actual_accepted_digest != accepted_digest:
        raise ValueError("accepted artifact bytes do not match the pinned digest")
    try:
        report = json.loads(content)
    except (ValueError, TypeError) as error:
        raise ValueError("accepted report envelope is invalid") from error
    markdown = report.get("markdown") if isinstance(report, dict) else None
    if (not isinstance(markdown, str) or not markdown
            or report.get("kind") != "verified_report@1"
            or report.get("revision") != revision):
        raise ValueError("accepted report does not contain the requested Markdown")
    markdown_bytes = markdown.encode("utf-8")
    return DeliveredMarkdown(
        content=markdown_bytes,
        markdown_sha256=hashlib.sha256(markdown_bytes).hexdigest(),
        accepted_sha256=accepted_digest,
        accepted_content_sha256=actual_accepted_digest,
        revision=revision,
    )
