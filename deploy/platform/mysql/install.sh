#!/usr/bin/env bash
set -e

groupadd mysql
useradd -g mysql mysql

mkdir /data && echo '/dev/vdb /data auto defaults 0 0 ' >> /etc/fstab  && mount -a
chown -R mysql:mysql /data

apt -y install mysql-server

service mysql stop
sudo cp -Rp /var/lib/mysql /data/mysql
sed -i "s/.*bind-address.*/bind-address = 0.0.0.0/" /etc/mysql/mysql.conf.d/mysqld.cnf
sed -i "s/.*datadir.*/datadir = \/data\/mysql/" /etc/mysql/mysql.conf.d/mysqld.cnf
sed -i "s/\/var\/lib\/mysql\//\/data\/mysql\//" /etc/apparmor.d/usr.sbin.mysqld
service apparmor reload
service mysql start

MYSQL_PASSWORD=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13 ; echo '')
mysql -uroot -e "DROP USER IF EXISTS 'app.rw'; CREATE USER 'app.rw'@'%' IDENTIFIED WITH mysql_native_password BY '${MYSQL_PASSWORD}'; GRANT ALL PRIVILEGES ON *.* TO 'app.rw'@'%' WITH GRANT OPTION;"

echo "MySQL credentials: app.rw:$MYSQL_PASSWORD"
