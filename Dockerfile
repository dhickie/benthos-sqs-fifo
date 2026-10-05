FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine AS build
ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -v -o /usr/local/bin/app ./src

FROM build AS test
ENTRYPOINT ["go", "test", "-tags", "unit,integration,stress,e2e", "-timeout", "30s", "-v", "./..."]

FROM alpine:latest AS run
COPY --from=build /usr/local/bin/app /usr/local/bin/app
ENTRYPOINT ["app"]

