.PHONY: up down logs test test-unit test-e2e resync status restart clean

up:
	docker-compose up --build -d

down:
	docker-compose down

logs:
	docker-compose logs -f

test:
	docker-compose run --rm test-runner

test-unit:
	docker-compose run --rm test-runner pytest tests/test_extractors.py tests/test_chunker.py -v

test-e2e:
	docker-compose run --rm test-runner pytest tests/test_e2e.py -v

resync:
	curl -X POST http://localhost:8100/resync

status:
	curl http://localhost:8100/status

restart:
	docker-compose restart ingest-worker

clean:
	docker-compose down -v
