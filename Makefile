run-test:
	poetry run pytest tests/test_producer.py --asyncio-mode=auto --cov=redisaq