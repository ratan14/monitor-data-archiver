FROM golang:1.18
RUN go install github.com/githubnemo/CompileDaemon@latest
RUN go install github.com/go-delve/delve/cmd/dlv@latest
RUN curl -sLo /tmp/aws-lambda-rie https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie && mv /tmp/aws-lambda-rie /usr/local/bin/aws-lambda-rie && chmod +x /usr/local/bin/aws-lambda-rie
WORKDIR /src
COPY . .

ENTRYPOINT CompileDaemon -log-prefix=false -build="go build -o /var/task/main" -command="/usr/local/bin/aws-lambda-rie /var/task/main"