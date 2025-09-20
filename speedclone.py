#!/usr/bin/env python3
from __future__ import annotations
import argparse, io, json, os, re, sys, tarfile, zipfile, time
from pathlib import Path
from urllib.parse import urlparse, urlencode
from urllib.request import Request, urlopen
from rich import print

VERSION = "1.0"
UA = f"speedclone/{VERSION}"

def _http(url, accept=None, token=None):
    h = {"User-Agent": UA}
    if accept: h["Accept"] = accept
    if token: h["Authorization"] = f"Bearer {token}"
    return urlopen(Request(url, headers=h))

def _json(url, token=None):
    with _http(url, "application/vnd.github+json", token) as r:
        return json.loads(r.read().decode("utf-8"))

def _read(url):
    with _http(url) as r:
        return r.read()

def _owner_repo(url: str):
    u = urlparse(url)
    if "github.com" not in u.netloc.lower(): raise SystemExit("GitHub HTTPS URLのみ対応しています。")
    parts = [p for p in u.path.split("/") if p]
    if len(parts) < 2: raise SystemExit("不正なURLです。")
    owner, repo = parts[-2], parts[-1]
    if repo.endswith(".git"): repo = repo[:-4]
    return owner, repo

def _guess_default_sha_html(owner, repo, branch_hint=None):
    def scrape(path):
        html = _read(f"https://github.com/{owner}/{repo}/{path}").decode("utf-8","ignore")
        return html
    for b in ([branch_hint] if branch_hint else []) + ["main","master"]:
        try:
            html = scrape(f"commits/{b}")
            m = re.search(rf'href="/{re.escape(owner)}/{re.escape(repo)}/commit/([0-9a-f]{{40}})"', html, re.I)
            if m: return b, m.group(1)
        except Exception:
            pass
    for b in ([branch_hint] if branch_hint else []) + ["main","master"]:
        try:
            html = scrape(f"tree/{b}")
            m = re.search(r'data-test-selector="commit-tease-sha".*?>([0-9a-f]{7,40})<', html, re.I|re.S)
            if m: return b, m.group(1)
        except Exception:
            pass
    raise RuntimeError("HTMLから最新SHAを取得できませんでした。")

def _is_within(base: Path, target: Path) -> bool:
    try:
        base = base.resolve(strict=False)
        target = target.resolve(strict=False)
        return str(target).startswith(str(base) + os.sep)
    except Exception:
        return False

def _download_tar_stream(url: str, dst: Path):
    with _http(url) as resp:
        with tarfile.open(fileobj=resp, mode="r|gz") as tf:
            _extract_tar_to(tf, dst)

def _extract_tar_to(tf: tarfile.TarFile, dst: Path):
    prefix = None
    for m in tf.getmembers():
        if m.isdir():
            prefix = m.name.rstrip('/') + '/'
            break
    if prefix is None: raise RuntimeError("tarの解凍時にエラー: 構造が不正です。")
    for m in tf.getmembers():
        
        if not m.name.startswith(prefix): continue
        rel = m.name[len(prefix):]
        if not rel: continue
        out = dst / rel
        if not _is_within(dst, out):
            continue
        if m.issym() or m.islnk():
            continue
        if m.isdir():
            out.mkdir(parents=True, exist_ok=True)
        elif m.isfile():
            out.parent.mkdir(parents=True, exist_ok=True)
            src = tf.extractfile(m)
            if src is None: continue
            with src, open(out, "wb") as f:
                f.write(src.read())
            try:
                os.chmod(out, m.mode & 0o777)
            except Exception:
                pass

def _extract_zip_to(zf: zipfile.ZipFile, dst: Path):
    roots = sorted({name.split('/',1)[0]+'/' for name in zf.namelist() if '/' in name})
    prefix = roots[0] if roots else ''
    for name in zf.namelist():
        if prefix and not name.startswith(prefix):
            continue
        rel = name[len(prefix):]
        if not rel:
            continue
        out = (dst / rel).resolve(strict=False)

        if not _is_within(dst, out):
            continue

        if name.endswith('/'):
            out.mkdir(parents=True, exist_ok=True)
        else:
            out.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(name) as src, open(out,"wb") as f:
                f.write(src.read())
            try:
                info = zf.getinfo(name)
                perm = (info.external_attr >> 16) & 0o777
                if perm:
                    os.chmod(out, perm)
            except Exception:
                pass

def _download_snapshot(dst: Path, owner: str, repo: str, ref_sha: str, branch_name: str):
    errs = []
    try:
        _download_tar_stream(f"https://codeload.github.com/{owner}/{repo}/tar.gz/{ref_sha}", dst)
        return
    except Exception as e:
        errs.append(f"tar.gz(stream)@sha: {e}")
    try:
        _download_tar_stream(f"https://codeload.github.com/{owner}/{repo}/tar.gz/{branch_name}", dst)
        return
    except Exception as e:
        errs.append(f"tar.gz(stream)@branch: {e}")
    def fetch_bytes(url: str) -> bytes:
        data = _read(url)
        if not data: raise RuntimeError("空の応答が返されました。")
        return data
    try:
        data = fetch_bytes(f"https://codeload.github.com/{owner}/{repo}/tar.gz/{ref_sha}")
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            _extract_tar_to(tf, dst); return
    except Exception as e: errs.append(f"tar.gz@sha: {e}")
    try:
        data = fetch_bytes(f"https://codeload.github.com/{owner}/{repo}/tar.gz/{branch_name}")
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            _extract_tar_to(tf, dst); return
    except Exception as e: errs.append(f"tar.gz@branch: {e}")
    try:
        data = fetch_bytes(f"https://codeload.github.com/{owner}/{repo}/zip/{ref_sha}")
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            _extract_zip_to(zf, dst); return
    except Exception as e: errs.append(f"zip@sha: {e}")
    try:
        data = fetch_bytes(f"https://codeload.github.com/{owner}/{repo}/zip/{branch_name}")
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            _extract_zip_to(zf, dst); return
    except Exception as e: errs.append(f"zip@branch: {e}")
    raise RuntimeError("スナップショットのダウンロードに失敗しました: " + "; ".join(errs))

def _write_git_skeleton(dst: Path, remote_url: str, default_branch: str, head_sha: str):
    git = dst / ".git"
    (git / "refs/heads").mkdir(parents=True, exist_ok=True)
    (git / "refs/remotes/origin").mkdir(parents=True, exist_ok=True)
    (git / "refs/remotes/upstream").mkdir(parents=True, exist_ok=True)
    (git / "objects/info").mkdir(parents=True, exist_ok=True)
    (git / "HEAD").write_text(f"ref: refs/heads/{default_branch}\n", encoding="utf-8")
    (git / "refs/heads" / default_branch).write_text(head_sha + "\n", encoding="ascii")
    (git / "refs/remotes/origin" / default_branch).write_text(head_sha + "\n", encoding="ascii")
    (git / "refs/remotes/upstream" / default_branch).write_text(head_sha + "\n", encoding="ascii")
    cfg = f"""
[core]
\trepositoryformatversion = 0
\tfilemode = true
\tbare = false
\tlogallrefupdates = true
[remote "origin"]
\turl = {remote_url}
\tfetch = +refs/heads/*:refs/remotes/origin/*
\tpromisor = true
\tpartialclonefilter = blob:none
[remote "upstream"]
\turl = {remote_url}
\tfetch = +refs/heads/*:refs/remotes/upstream/*
\tpromisor = true
\tpartialclonefilter = blob:none
[branch "{default_branch}"]
\tremote = origin
\tmerge = refs/heads/{default_branch}
[extensions]
\tpartialClone = origin
""".lstrip()
    (git / "config").write_text(cfg, encoding="utf-8")
    (git / "objects/info/promisor").write_text("promisor\n", encoding="utf-8")

def bootstrap(repo_url: str, dst: Path):
    print("[blue](i)[/blue] 引数等の問題はありません。開始します...")
    owner, repo = _owner_repo(repo_url)
    print(f"[purple](i)[/purple] owner: {owner}, repo: {repo}")
    token = os.environ.get("GITHUB_TOKEN")
    default_branch = "master"
    head_sha = None
    try:
        meta = _json(f"https://api.github.com/repos/{owner}/{repo}", token)
        default_branch = meta.get("default_branch") or "master"
        tip = _json(f"https://api.github.com/repos/{owner}/{repo}/commits/{default_branch}", token)
        head_sha = tip["sha"]
        print(f"[purple](i)[/purple] default_branch: {default_branch}, head: {head_sha[:12]}… (APIで取得)")
    except Exception:
        print("[yellow](!)[/yellow] API経由でのダウンロード情報の取得に失敗しました。HTMLでの取得を試行します...")
        b, sha = _guess_default_sha_html(owner, repo)
        default_branch, head_sha = b, sha
        print(f"[purple](i)[/purple] default_branch: {default_branch}, head: {head_sha[:12]}… (HTMLで取得)")

    print("[blue](i)[/blue] スナップショットのダウンロードを開始します。")
    t0 = time.time()
    _download_snapshot(dst, owner, repo, head_sha, default_branch)
    print(f"[blue](i)[/blue] スナップショットはcodeloadを使用して{time.time()-t0:.1f}秒でダウンロードされました。")

    _write_git_skeleton(dst, repo_url, default_branch, head_sha)
    print("[green](✓)[/green] 完了しました。")

def main():
    ap = argparse.ArgumentParser(description="Bootstrap-clone huge/deep GitHub repos without hitting protocol/API quirks.")
    ap.add_argument("url"); ap.add_argument("target")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    dst = Path(args.target)
    if dst.exists():
        if not args.force:
            print(f"[yellow](!)[/yellow] ディレクトリが既に存在しています: {dst}", file=sys.stderr); sys.exit(1)
        for p in sorted(dst.rglob("*"), reverse=True):
            try: p.unlink()
            except IsADirectoryError: pass
        for p in sorted(dst.rglob("*"), reverse=True):
            try: p.rmdir()
            except Exception: pass
    dst.mkdir(parents=True, exist_ok=True)
    try:
        bootstrap(args.url, dst)
    except Exception as e:
        print("[red](!)[/red] 例外が発生しました:", e, file=sys.stderr); sys.exit(2)

if __name__ == "__main__":
    main()
