#!/usr/bin/env bash
[[ $(id -u) -eq 0 ]] || exec sudo /bin/bash -c "$(printf '%q ' "$BASH_SOURCE" "$@")"

systemctl disable apache2
systemctl stop apache2

set -e

#mkdir /var/lib/containerd && echo '/dev/vdb /var/lib/containerd auto defaults 0 0 ' >> /etc/fstab  && mount -a && ll /var/lib/containerd

ufw disable

sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab
swapoff -a

apt update
apt -y install apt-transport-https ca-certificates curl
curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg
echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | tee /etc/apt/sources.list.d/kubernetes.list
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null

apt update
apt -y install vim git wget kubelet kubeadm kubectl containerd.io rng-tools
apt-mark hold kubelet kubeadm kubectl

sed -i 's/hwrng/urandom/g' /lib/systemd/system/rng-tools.service
service rng-tools restart

cat <<EOF | tee /etc/modules-load.d/k8s.conf
overlay
br_netfilter
EOF

modprobe overlay
modprobe br_netfilter

tee /etc/sysctl.d/kubernetes.conf<<EOF
net.bridge.bridge-nf-call-ip6tables = 1
net.bridge.bridge-nf-call-iptables = 1
net.ipv4.ip_forward = 1
EOF

sysctl --system

mkdir -p /etc/containerd
containerd config default>/etc/containerd/config.toml
systemctl restart containerd
systemctl enable containerd

systemctl enable kubelet
kubeadm config images pull
