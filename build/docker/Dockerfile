FROM alpine:3.9

USER nobody

ADD build/_output/etcd-raft-replica /usr/local/bin/etcd-raft-replica

ENTRYPOINT ["etcd-raft-replica"]
