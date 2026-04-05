FROM node:20-alpine

# Cài docker-cli để control containers qua socket
RUN apk add --no-cache docker-cli

WORKDIR /app

# Chỉ copy file cần thiết
COPY elector.js ./

# Không cần npm install — chỉ dùng Node.js built-in modules:
#   https, http, crypto, child_process

ENTRYPOINT ["node", "elector.js"]