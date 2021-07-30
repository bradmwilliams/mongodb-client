FROM registry.ci.openshift.org/openshift/release:golang-1.16 AS builder
WORKDIR /go/src/github.com/bradmwilliams/mongodb-client
COPY . .
RUN make

FROM registry.ci.openshift.org/openshift/mongodb:latest
COPY --from=builder /go/src/github.com/bradmwilliams/mongodb-client/mongodb-client /usr/bin/
#ENTRYPOINT ["/usr/bin/prowconfig-tester"]
CMD ["/usr/bin/mongodb-client"]
