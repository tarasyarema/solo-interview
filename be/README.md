# The BE

## Stress testing

```
poetry install
poetry run fastapi run --workers={n > 1}
poetry run python stress.py
```
