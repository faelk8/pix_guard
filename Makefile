.PHONY: build up down test config storage-check

build:
	docker compose build

up:
	docker compose up -d --wait

down:
	docker compose down

test:
	docker compose run --rm --no-deps app pytest

config:
	docker compose config --quiet

storage-check:
	docker compose run --rm --no-deps app pixguard check-storage
