.PHONY: all
all:

.PHONY: test
test:
	coverage run -m unittest discover -s tests
	coverage report

.PHONY: coverage
coverage: test
	coverage html
	open ./htmlcov/index.html

.PHONY: synch
synch:
	python3 setup.py sdist
	./dbw synch workflow_example

.PHONY:
run: synch
	./dbw run workflow_example

.PHONY: run-task
run-task: synch
	./dbw run-task workflow_example dlt_sample_task

.PHONY: destroy
destroy: synch
	./dbw delete workflow_example
