# manually freeing up ports
for port in {8000..8009} {9000..9009}; do
  pid=$(lsof -t -i :$port)
  if [ -n "$pid" ]; then
    echo "Releasing port $port occupied by PID $pid"
    kill -9 $pid
  fi
done
