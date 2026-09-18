# sales-pipeline

Pipeline PySpark orienté objet pour analyser les ventes d'une entreprise
e-commerce, exécuté par AWS Glue, avec CI/CD via GitHub Actions.

## Structure
- `src/sales_pipeline/` : code métier PySpark (OOP), sans dépendance AWS
  directe (sauf le point d'entrée `job.py::main`).
- `tests/` : tests pytest (SparkSession locale).
- `scripts/generate_sales_data.py` : génère un jeu de données factice.
- `infra/` : fonctions Lambda (déclenchement Glue, déclenchement Crawler).
- `.github/workflows/` : CI (tests) et CD (déploiement du script vers S3).

## Lancer les tests en local
pip install -r requirements.txt
pytest -v
