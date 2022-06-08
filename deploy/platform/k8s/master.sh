#!/usr/bin/env bash
[[ $(id -u) -eq 0 ]] || exec sudo /bin/bash -c "$(printf '%q ' "$BASH_SOURCE" "$@")"

address=$(hostname -I | awk '{print $1}')
echo "$address kube.cluster.depot" >> /etc/hosts

sudo kubeadm init \
  --pod-network-cidr=192.168.0.0/16 \
  --upload-certs \
  --control-plane-endpoint=kube.cluster.depot

kubectl create -f https://docs.projectcalico.org/manifests/tigera-operator.yaml
kubectl create -f calico_vxlan.yaml

host_command='echo "'"$address"' kube.cluster.depot" >> /etc/hosts'
join_command=$(kubeadm token create --print-join-command)
echo "Join command for workers:"
echo "  $host_command && $join_command"
