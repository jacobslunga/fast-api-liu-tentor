from supabase import create_client
from dotenv import load_dotenv
import base64
import os
import math
import re
import unicodedata
from datetime import datetime

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(url, key)

SAVE_PATH = "pdfs"
os.makedirs(SAVE_PATH, exist_ok=True)

BATCH_SIZE = 500


def _strip_diacritics(s: str) -> str:
    n = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in n if not unicodedata.combining(ch))


def is_facit(name: str) -> bool:
    normalized_name = _strip_diacritics(name).lower()
    is_facit_pattern = re.compile(r"^l\d|^l_\d{8}")

    facit_keywords = [
        "losningsforslag",
        "facit",
        "solution",
        "sol",
        "losning",
        "lsn",
        "losnings",
        "losnings",
        "losning",
        "tenlsg",
        "lf",
        "_l",
        "svar",
    ]

    is_facit_keyword = (
        any(k in normalized_name for k in facit_keywords)
        and "tenta_och_svar" not in normalized_name
    )
    return bool(is_facit_pattern.search(normalized_name) or is_facit_keyword)


def extract_date_from_name(name: str):
    patterns = [
        r"(\d{4})-(\d{2})-(\d{2})",
        r"(\d{4})(\d{2})(\d{2})",
        r"(\d{2})(\d{2})(\d{2})",
        r"(\d{2})_(\d{2})_(\d{2})",
        r"(\d{4})_(\d{2})_(\d{2})",
        r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})",
        r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})",
        r"(?:jan|feb|mar|apr|maj|jun|jul|aug|sep|okt|nov|dec)[a-z]*[-_](\d{2,4})",
        r"(\d{2,4})[-_](?:jan|feb|mar|apr|maj|jun|jul|aug|sep|okt|nov|dec)[a-z]*",
        r"T?(\d{1,2})[-_](\d{4})",
        r"HT(\d{2})",
        r"VT(\d{2})",
    ]
    month_map = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "maj": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "okt": "10",
        "nov": "11",
        "dec": "12",
    }

    name_lower = name.lower()
    for pattern in patterns:
        match = re.search(pattern, name_lower)
        if not match:
            continue
        try:
            if match.group(0).startswith("ht"):
                year = f"20{match.group(1)}"
                return datetime(int(year), 12, 1)
            elif match.group(0).startswith("vt"):
                year = f"20{match.group(1)}"
                return datetime(int(year), 1, 1)
            elif "t" in match.group(0) and len(match.groups()) == 2:
                month = "01" if match.group(1) == "1" else "06"
                return datetime(int(match.group(2)), int(month), 1)
            else:
                year, month, day = match.groups()
                if len(year) == 2:
                    year = f"20{year}"
                if month.lower() in month_map:
                    month = month_map[month.lower()]
                return datetime(int(year), int(month), int(day))
        except:
            continue
    return None


def download_pdfs():
    total = supabase.table("tentor").select("id", count="exact").execute().count
    pages = math.ceil(total / BATCH_SIZE)

    for page in range(pages):
        start = page * BATCH_SIZE
        end = start + BATCH_SIZE - 1

        tentor = (
            supabase.table("tentor")
            .select("id, kurskod, tenta_namn, document_id")
            .range(start, end)
            .execute()
            .data
        )

        for tenta in tentor:
            doc = (
                supabase.table("documents")
                .select("content")
                .eq("id", tenta["document_id"])
                .execute()
                .data
            )

            if not doc:
                continue

            date = extract_date_from_name(
                tenta["tenta_namn"].replace(tenta["kurskod"], "")
            )
            if not date:
                print(f"Skipping (no date): {tenta['tenta_namn']}")
                continue

            exam_type = "SOLUTION" if is_facit(tenta["tenta_namn"]) else "EXAM"
            filename = f"{tenta['kurskod']}_{date.strftime('%Y-%m-%d')}_{exam_type}.pdf"

            pdf_bytes = base64.b64decode(doc[0]["content"])
            with open(os.path.join(SAVE_PATH, filename), "wb") as f:
                f.write(pdf_bytes)

        print(f"Downloaded batch {page + 1}/{pages}")


download_pdfs()
