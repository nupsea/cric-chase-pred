import redis

# Start redis server: redis-server /opt/homebrew/etc/redis.conf

# Connect to your Redis instance
r = redis.Redis(host="localhost", port=6379, db=0)

# List all keys (use with caution in production)
keys = r.keys("*")
print(keys)

# Retrieve data for each key
for key in keys:
    print(key, r.get(key))
