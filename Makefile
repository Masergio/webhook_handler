.DEFAULT_GOAL := run

IMAGE := postgres:11.2
DB_HOST := localhost
DB_PORT := 5432
DB_USER := postgres
DB_PASSWORD := mysecretpassword
CONTAINER_NAME := local-email-postgres
DB_NAME := webhook_events

PORTS := -p $(DB_PORT):$(DB_PORT)
ENVS := -e POSTGRES_PASSWORD=$(DB_PASSWORD) --name $(CONTAINER_NAME)
RUN_OPTS := $(PORTS) $(ENVS)

run:
	docker run -d $(PORTS) $(ENVS) $(IMAGE)
	sleep 4
	docker exec $(CONTAINER_NAME) createdb -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME)

start:
	docker start $(CONTAINER_NAME)

stop:
	docker stop $(CONTAINER_NAME)

remove: stop
	docker rm $(CONTAINER_NAME)

reset: stop start

hard_reset: remove run
