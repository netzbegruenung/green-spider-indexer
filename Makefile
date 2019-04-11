build:
	docker build -t quay.io/netzbegruenung/green-spider-indexer:latest .

run:
	docker run --rm -ti \
		-v ${PWD}/secrets:/etc/indexer \
		-e "GCLOUD_DATASTORE_CREDENTIALS_PATH=/etc/indexer/datastore-reader.json" \
		quay.io/netzbegruenung/green-spider-indexer:latest
