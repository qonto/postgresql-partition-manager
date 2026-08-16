from pathlib import Path
import re


def define_env(env):
    @env.macro
    def adr_list():
        docs_dir = Path(env.conf["docs_dir"])
        adr_dir = docs_dir / "adr"

        files = sorted(
            p for p in adr_dir.glob("*.md")
            if p.name.lower() != "index.md"
        )

        if not files:
            return "_No ADRs have been added yet._"

        rows = [
            "| ADR | Title | Status |",
            "|---|---|---|",
        ]

        for path in files:
            content = path.read_text(encoding="utf-8")
            title = extract_title(content, path)
            status = extract_status(content)

            rows.append(f"| `{path.stem}` | [{title}]({path.name}) | {status} |")

        return "\n".join(rows)


def extract_title(content: str, path: Path) -> str:
    match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
    return match.group(1).strip() if match else path.stem


def extract_status(content: str) -> str:
    match = re.search(r"^##\s+Status\s*\n+\s*([A-Za-z]+)", content, re.MULTILINE)
    return match.group(1).strip() if match else "Unknown"
