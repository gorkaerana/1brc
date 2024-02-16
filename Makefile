lint:
	ruff check

format:
	ruff format

type_check:
	mypy .

code_quality: format lint type_check

cq: code_quality
