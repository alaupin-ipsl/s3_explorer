"""Script d'analyse et de téléchargement d'un dépôt S3"""

import argparse
from collections import defaultdict
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from tqdm import tqdm

from config import BUCKET, ENDPOINT_URL

# Paramétrage des arguments
parser = argparse.ArgumentParser(description="Explorer un bucket S3 public et calculer des stats par dossier.")
parser.add_argument("--prefix", required=True, help="Préfixe S3 à explorer")
parser.add_argument("--extensions", nargs="*", help="Extensions à filtrer (ex : .tar .nc)")
parser.add_argument("--details", action="store_true", help="Afficher les stats des dossiers explorés")
parser.add_argument("--quiet", action="store_true", help="Désactive les logs des barres de progression")
parser.add_argument("--download", action="store_true", help="Télécharger les fichiers")
parser.add_argument("--dest", help="Dossier local de destination")

args = parser.parse_args()

if args.download and args.dest:
    DEST_ROOT = Path(args.dest)
elif args.download or args.dest:
    raise

# Normalisation des extensions
extensions = None
if args.extensions:
    extensions = [ext.lower() if ext.startswith(".") else f".{ext.lower()}" for ext in args.extensions]

PREFIX = args.prefix if args.prefix.endswith("/") else f"{args.prefix}/"

# Création du client S3
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    config=Config(signature_version=UNSIGNED),
)

# ------------------- #
# 1️⃣ Analyse du dépôt #
# ------------------- #

stats = defaultdict(lambda: {"files": 0, "bytes": 0, "ignored": 0})

total_files = 0
total_bytes = 0
total_ignored = 0

paginator = s3.get_paginator("list_objects_v2")

with tqdm(desc="🔎 Analyse du dépôt", unit=" fichier", disable=args.quiet) as analyse_progression_bar:
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            folder = Path(key).parent

            # Ignorer les objets de type "dossiers"
            if key.endswith("/"):
                continue

            # Ignorer les fichiers ne correspondant pas aux extensions exigées
            if extensions:
                ext = Path(key).suffix
                if ext.lower() not in extensions:
                    stats[folder]["ignored"] += 1
                    total_ignored += 1
                    continue

            # Incrémentation des stats
            stats[folder]["files"] += 1
            stats[folder]["bytes"] += size

            total_files += 1
            total_bytes += size

            # Mise à jour de la progression
            analyse_progression_bar.update()

# ------------------------------ #
# 2️⃣ Téléchargement des fichiers #
# ------------------------------ #

downloaded_files = 0
downloaded_bytes = 0
if args.download:
    # Réinitialise la pagination
    paginator = s3.get_paginator("list_objects_v2")

    with tqdm(total=total_bytes, desc="💾 Téléchargement", unit="B", unit_scale=True, disable=args.quiet) as progress_bar:
        for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                size = obj["Size"]

                # Ignorer les objets de type "dossiers"
                if key.endswith("/"):
                    continue

                # Ignorer les fichiers ne correspondant pas aux extensions exigées
                if extensions:
                    ext = Path(key).suffix
                    if ext.lower() not in extensions:
                        continue

                # Téléchargement
                local_path = DEST_ROOT / key
                local_path.parent.mkdir(parents=True, exist_ok=True)

                s3.download_file(BUCKET, key, str(local_path))
                downloaded_bytes += size
                downloaded_files += 1

                # Mise à jour de la progression
                progress_bar.set_postfix(
                    {"Fichiers téléchargés": downloaded_files},
                )
                progress_bar.update(size)


# ---------- #
# 3️⃣ Rapport #
# ---------- #

print(f"📁 Statistiques du contenu du dossier {PREFIX}")

if args.details:
    for folder in sorted(stats):
        files = stats[folder]["files"]
        size_gb = stats[folder]["bytes"] / (1024**3)
        ignored = stats[folder]["ignored"]
        print(f"    {folder}")
        print(f"      ├─ fichiers : {files}")
        print(f"      ├─ taille   : {size_gb:.2f} Go")
        print(f"      └─ ignorés   : {ignored}")
        print()

print("    📂 TOTAL")
print(f"    ├─ fichiers : {total_files}")
print(f"    ├─ taille   : {total_bytes / (1024 ** 3):.2f} Go")
print(f"    └─ ignorés   : {total_ignored}")
if args.download:
    if downloaded_files == 1:
        print(f"💾 1 fichier téléchargé ({downloaded_bytes / (1024**3):.2f} Go) dans : {DEST_ROOT.resolve()}")
    else:
        print(f"💾 {downloaded_files} fichiers téléchargés ({downloaded_bytes / (1024**3):.2f} Go) dans : {DEST_ROOT.resolve()}")
