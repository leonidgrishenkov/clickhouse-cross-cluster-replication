.PHONY: s1-up s1-down s1-test s1-clean \
        s6-up s6-down s6-test s6-clean \
        s3-up s3-down s3-test s3-clean \
        s9-up s9-down s9-test s9-clean \
        s7-up s7-down s7-test s7-clean \
        clean down

CH_VERSION := 25.8.30.16

s1-up:
	cd docker/s1 && docker compose up -d keeper-1 keeper-2 keeper-3 \
		ch-main-s1r1 ch-main-s1r2 ch-main-s2r1 ch-main-s2r2 \
		ch-ext-s1r1 ch-ext-s2r1
	@echo "Waiting for nodes to become ready..."
	sleep 12
	bash scripts/bootstrap_s1.sh

s1-up-s1c:
	cd docker/s1 && docker compose --profile s1c up -d keeper-ext-1
	sleep 5

s1-test:
	bash scripts/run_scenario_test.sh s1

s1-down:
	cd docker/s1 && docker compose down

s1-clean:
	cd docker/s1 && docker compose down -v

s6-up:
	cd docker/s6 && docker compose up -d
	sleep 12
	bash scripts/bootstrap_s6.sh

s6-test:
	bash scripts/run_scenario_test.sh s6

s6-down:
	cd docker/s6 && docker compose down

s6-clean:
	cd docker/s6 && docker compose down -v

s3-up:
	cd docker/s3 && docker compose up -d
	sleep 12
	bash scripts/bootstrap_s3.sh

s3-test:
	bash scripts/run_scenario_test.sh s3

s3-down:
	cd docker/s3 && docker compose down

s3-clean:
	cd docker/s3 && docker compose down -v

s9-up:
	cd docker/s9 && docker compose up -d
	sleep 12
	bash scripts/bootstrap_s9.sh

s9-test:
	bash scripts/run_scenario_test.sh s9

s9-down:
	cd docker/s9 && docker compose down

s9-clean:
	cd docker/s9 && docker compose down -v

s7-up:
	cd docker/s7 && docker compose up -d
	sleep 12
	bash scripts/bootstrap_s7.sh

s7-test:
	bash scripts/run_scenario_test.sh s7

s7-down:
	cd docker/s7 && docker compose down

s7-clean:
	cd docker/s7 && docker compose down -v

down: s1-down s6-down s3-down s9-down s7-down

clean: s1-clean s6-clean s3-clean s9-clean s7-clean
	docker system prune -f --volumes --filter "label=project=ch-cross-cluster-repl" || true
