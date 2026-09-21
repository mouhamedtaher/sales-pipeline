"""Construit sales_pipeline.zip à partir de src/sales_pipeline/.

Utilise le module zipfile de Python plutôt que des outils système
(zip, Compress-Archive) pour garantir des séparateurs de chemin '/'
dans l'archive, quel que soit l'OS -- AWS Glue (Linux) ne sait pas
importer un package zippé avec des séparateurs Windows '\\'.
"""
import os
import zipfile

SRC_DIR = "src"
PACKAGE_DIR = os.path.join(SRC_DIR, "sales_pipeline")
OUTPUT_ZIP = "sales_pipeline.zip"


def build_zip():
    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(PACKAGE_DIR):
            for filename in files:
                if filename.endswith(".pyc"):
                    continue
                filepath = os.path.join(root, filename)
                arcname = os.path.relpath(filepath, SRC_DIR).replace(os.sep, "/")
                zf.write(filepath, arcname)
    print(f"{OUTPUT_ZIP} created.")


if __name__ == "__main__":
    build_zip()
