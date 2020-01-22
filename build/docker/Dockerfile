FROM alpine:3.9

USER nobody

ADD build/_output/atomix-etcd-raft-node /usr/local/bin/atomix-etcd-raft-node

ENTRYPOINT ["atomix-etcd-raft-node"]
