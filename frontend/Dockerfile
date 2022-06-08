FROM nginx

ADD frontend/build /opt/depot-frontend
ADD frontend/conf/depot-frontend /etc/nginx/conf.d/default.conf
RUN chmod -R 755 /opt/depot-frontend
