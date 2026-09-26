#!/usr/bin/env python3
"""
从 nyas1/Material-You-app-list 的 README 中提取所有 GitHub 应用仓库，
下载其最新 release 中的所有 APK（不区分架构），流式直传到 Cloudflare R2 存储桶。

全部操作在 GitHub Actions workflow 内完成：解析 README → 拉取 release → 流式直传 R2，
不写本地磁盘。每个应用一个文件夹（owner-repo 命名），没有 release 或没有 APK 的应用不创建文件夹。
"""

import os
import re
import sys
import time
import logging
from typing import List, Tuple, Optional, Dict, Any

import requests
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET")
R2_PUBLIC_URL = os.environ.get("R2_PUBLIC_URL", "").rstrip("/")

ONLY_MISSING = os.environ.get("ONLY_MISSING", "true").lower() == "true"
CLEAN_STALE = os.environ.get("CLEAN_STALE", "true").lower() == "true"

README_PATH = os.environ.get("README_PATH", "app-list/README.md")

# Material-You-app-list 自身仓库，需要排除
SELF_REPO = "nyas1/material-you-app-list"

# 匹配形如：- `MDY` [AppName](https://github.com/owner/repo) ...
# 仅匹配列表项中、带有反引号标签的条目，从而排除 Guide / Setup 等非应用链接
APP_LINK_RE = re.compile(
    r"-\s*`[^`]+`\s*\[([^\]]+)\]\((https://github\.com/([^/\s)]+)/([^/\s)]+?))/?\)"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sync")


# ---------------------------------------------------------------------------
# README 解析
# ---------------------------------------------------------------------------
def parse_repos(readme_path: str) -> List[Tuple[str, str, str]]:
    """解析 README，返回 [(app_name, owner, repo), ...]，去重。"""
    if not os.path.exists(readme_path):
        log.error("README 不存在: %s", readme_path)
        sys.exit(1)

    with open(readme_path, "r", encoding="utf-8") as f:
        content = f.read()

    repos: List[Tuple[str, str, str]] = []
    seen = set()

    for m in APP_LINK_RE.finditer(content):
        app_name, _url, owner, repo = m.groups()
        # 去除仓库名末尾可能残留的点号等
        repo = repo.rstrip(".,/")
        full = f"{owner}/{repo}".lower()

        if full == SELF_REPO:
            continue
        if full in seen:
            continue
        seen.add(full)
        repos.append((app_name.strip(), owner, repo))

    return repos


# ---------------------------------------------------------------------------
# GitHub API
# ---------------------------------------------------------------------------
def _gh_headers() -> Dict[str, str]:
    h = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


def get_latest_release(owner: str, repo: str) -> Optional[Dict[str, Any]]:
    """
    获取最新 release。
    优先 /releases/latest（最新非预发布）；
    若 404 则回退到 /releases?per_page=1（包含预发布）。
    """
    base = f"https://api.github.com/repos/{owner}/{repo}"

    # 1. 最新稳定版
    r = requests.get(f"{base}/releases/latest", headers=_gh_headers(), timeout=30)
    if r.status_code == 200:
        return r.json()
    if r.status_code not in (404, 403):
        log.debug("  releases/latest -> %s", r.status_code)

    # 2. 回退：取列表第一条（可能是预发布）
    r = requests.get(
        f"{base}/releases?per_page=1", headers=_gh_headers(), timeout=30
    )
    if r.status_code == 200:
        data = r.json()
        if data:
            return data[0]

    if r.status_code == 404:
        return None
    if r.status_code == 403:
        # 可能触发速率限制
        log.warning("  GitHub API 403 (可能速率限制): %s/%s", owner, repo)
        return None

    log.debug("  releases list -> %s", r.status_code)
    return None


def get_apk_assets(release: Dict[str, Any]) -> List[Dict[str, Any]]:
    """从 release 中筛选所有 .apk 资源。"""
    assets = release.get("assets", []) or []
    return [a for a in assets if a["name"].lower().endswith(".apk")]


def stream_asset_to_r2(asset: Dict[str, Any], s3, key: str):
    """从 GitHub release 流式下载并直接上传到 R2，不写本地磁盘。"""
    url = asset["browser_download_url"]
    headers = {"Accept": "application/octet-stream"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    # 流式直传：requests 的响应流直接喂给 boto3 上传
    config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=False,
    )

    with requests.get(url, stream=True, headers=headers, timeout=300) as r:
        r.raise_for_status()
        r.raw.decode_content = True
        s3.upload_fileobj(r.raw, R2_BUCKET, key, Config=config)


# ---------------------------------------------------------------------------
# R2 操作
# ---------------------------------------------------------------------------
def make_r2_client():
    endpoint = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


def r2_object_exists(s3, key: str, expected_size: int) -> bool:
    """检查 R2 中是否已存在相同大小的对象。"""
    try:
        head = s3.head_object(Bucket=R2_BUCKET, Key=key)
        return head["ContentLength"] == expected_size
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def list_folder_objects(s3, prefix: str) -> List[str]:
    """列出 R2 中指定前缀下的所有对象 key。"""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            keys.append(obj["Key"])
    return keys


def delete_keys(s3, keys: List[str]):
    """批量删除 R2 对象（R2 支持的最大批量为 1000）。"""
    if not keys:
        return
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        s3.delete_objects(
            Bucket=R2_BUCKET,
            Delete={"Objects": [{"Key": k} for k in batch]},
        )


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def process_app(s3, app_name: str, owner: str, repo: str) -> bool:
    """处理单个应用，返回是否成功上传了 APK。"""
    full = f"{owner}/{repo}"
    folder = f"{owner}-{repo}"  # R2 中用 owner-repo 作为文件夹名，避免斜杠嵌套
    log.info("处理: %s (%s)", app_name, full)

    release = get_latest_release(owner, repo)
    if not release:
        log.info("  无 release，跳过")
        return False

    tag = release.get("tag_name", "?")
    apks = get_apk_assets(release)
    if not apks:
        log.info("  release %s 中无 APK 资源，跳过", tag)
        return False

    log.info("  最新 release: %s，共 %d 个 APK", tag, len(apks))

    uploaded_keys = set()

    for asset in apks:
        name = asset["name"]
        size = asset["size"]
        key = f"{folder}/{name}"

        if ONLY_MISSING and r2_object_exists(s3, key, size):
            log.info("    [跳过] %s 已存在（大小一致）", name)
            uploaded_keys.add(key)
            continue

        log.info("    流式直传到 R2: %s (%s bytes)", name, size)
        try:
            stream_asset_to_r2(asset, s3, key)
            uploaded_keys.add(key)
        except Exception as e:
            log.error("    传输失败: %s", e)
            continue

        # 避免触发速率限制
        time.sleep(0.2)

    if not uploaded_keys:
        log.info("  本应用没有成功上传任何 APK")
        return False

    # 清理该文件夹下不属于当前 release 的旧 APK
    if CLEAN_STALE:
        existing = set(list_folder_objects(s3, f"{folder}/"))
        stale = existing - uploaded_keys
        if stale:
            log.info("  清理 %d 个旧 APK: %s", len(stale), [k.split("/")[-1] for k in stale])
            delete_keys(s3, list(stale))

    if R2_PUBLIC_URL:
        log.info("  可访问: %s/%s/", R2_PUBLIC_URL, folder)

    return True


def main():
    missing = [
        name
        for name, val in [
            ("R2_ACCOUNT_ID", R2_ACCOUNT_ID),
            ("R2_ACCESS_KEY_ID", R2_ACCESS_KEY_ID),
            ("R2_SECRET_ACCESS_KEY", R2_SECRET_ACCESS_KEY),
            ("R2_BUCKET", R2_BUCKET),
        ]
        if not val
    ]
    if missing:
        log.error("缺少必需的环境变量: %s", ", ".join(missing))
        sys.exit(1)

    repos = parse_repos(README_PATH)
    log.info("从 README 中提取到 %d 个 GitHub 应用仓库", len(repos))

    s3 = make_r2_client()

    ok = 0
    skip = 0
    fail = 0

    for app_name, owner, repo in repos:
        try:
            if process_app(s3, app_name, owner, repo):
                ok += 1
            else:
                skip += 1
        except Exception as e:
            log.exception("处理 %s/%s 时出错: %s", owner, repo, e)
            fail += 1

    log.info("=" * 50)
    log.info("完成：成功 %d，跳过(无release/无APK) %d，失败 %d", ok, skip, fail)


if __name__ == "__main__":
    main()
