# Makefile — Australian Leads Ingestion

.PHONY: help install test clean start stop logs schema run-all run-city

help:
	@echo "Available targets:"
	@echo "  install    — Install Python deps (pip install -r requirements.txt)"
	@echo "  test       — Run dry-run ingestion for all cities"
	@echo "  schema     — Apply schema to Supabase (requires supabase CLI)"
	@echo "  run-all    — Ingest all 8 cities"
	@echo "  run-city   — Ingest single city (set CITY=sydney)"
	@echo "  start      — Start local Supabase via Docker"
	@echo "  stop       — Stop local Supabase"
	@echo "  logs       — Show DB logs"
	@echo "  clean      — Remove local data volumes"

install:
	pip install -r requirements.txt

test:
	python ingestion_pipeline.py --all --dry-run

schema:
ifndef SUPABASE_URL
	@echo "ERROR: SUPABASE_URL not set"
	@exit 1
endif
	supabase db push schema/001_initial_schema.sql

run-all:
	python ingestion_pipeline.py --all

run-city:
ifndef CITY
	@echo "Usage: make run-city CITY=sydney"
	@exit 1
endif
	python ingestion_pipeline.py --city $(CITY)

start:
	docker-compose up -d postgres
	@echo "Waiting for DB..."
	@sleep 3
	@docker-compose exec -T postgres pg_isready -U postgres && echo "✅ DB ready"

stop:
	docker-compose down

logs:
	docker-compose logs -f postgres

clean:
	docker-compose down -v
	rm -rf data/outputs/*
