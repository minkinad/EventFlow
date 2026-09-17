.PHONY: build test verify format up down logs smoke e2e load-test clean
build:
	mvn -B -ntp package -DskipTests
test:
	mvn -B -ntp test
verify:
	mvn -B -ntp clean verify
format:
	mvn -B -ntp spotless:apply
up:
	docker compose up --build -d --wait
down:
	docker compose down
logs:
	docker compose logs -f --tail=100
smoke:
	./scripts/smoke-test.sh
e2e:
	mvn -B -ntp verify -Dit.test=EventLifecycleE2E -Dfailsafe.failIfNoSpecifiedTests=false -Djacoco.skip=true
load-test:
	k6 run scripts/load/events.js
clean:
	mvn -B -ntp clean
