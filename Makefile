PY=python3
run:
	PYTHONPATH=src $(PY) -m freshnessgate.cli
test:
	@if $(PY) -c "import pytest" 2>/dev/null; then \
		PYTHONPATH=src $(PY) -m pytest -q; \
	else \
		echo "pytest 不可用，回退到标准库 mini runner"; \
		$(PY) -I tests/test_freshness.py && $(PY) -I tests/test_pipeline.py; \
	fi
clean:
	rm -rf .pytest_cache **/__pycache__ *.db *.db-wal *.db-shm
