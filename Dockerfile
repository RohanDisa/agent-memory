FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/node ./cmd/node && CGO_ENABLED=0 go build -o /out/memctl ./cmd/memctl

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
COPY --from=build /out/node /usr/local/bin/node
COPY --from=build /out/memctl /usr/local/bin/memctl
EXPOSE 8080
ENTRYPOINT ["node"]
