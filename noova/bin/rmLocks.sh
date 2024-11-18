# __CLEAR__ in-mem locks using the cleanup endpoint in kvs -- debugging crawlers
cleanup_url="http://localhost:8000/cleanup"
response=$(curl -s -w "\n%{http_code}" "$cleanup_url")
http_code=$(echo "$response" | tail -n 1)
body=$(echo "$response" | head -n -1)

if [ "$http_code" -eq 200 ]; then
  echo "Cleanup endpoint called successfully. Response: $body"
else
  echo "Failed to call cleanup endpoint. HTTP status code: $http_code. Response: $body"
fi

#response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/cleanup)
#if [ "$response" -eq 200 ]; then
#  echo "Cleanup endpoint called successfully."
#else
#  echo "Failed to call cleanup endpoint. HTTP status code: $response"
#fi