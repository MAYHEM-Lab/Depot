INSERT INTO entities VALUES(2, 'alice', 'User', 'AKIAA5CSFDRLYW7VHIPR', 'mGW1V6qpcZmhCOH2o5nVCiom7gDcfU2va9qi33Hq', 0, 0);
INSERT INTO entities VALUES(3, 'bob', 'User', 'AKIAAWKPJ3DFAIM56ETC', '7fON11CN1Pz5rrdpP82ieLtY0Az4T3WG4AzsAjSW', 0, 0);
INSERT INTO entities VALUES(4, 'charlie', 'User', 'AKIAAUH53YVZSSXIYNBX', 'Ye7f9umUR0TjPUc1iTBQVzoeUIX6PVOi9U26curf', 0, 0);



# INSERT INTO github_user_link VALUES(2, 25595733);


insert into clusters values(1, 2, 'alice-cluster', 'Active', 0, 0);
insert into notebook_info VALUES(1, 'localhost:9998');
insert into spark_info VALUES(1, 'host.docker.internal:7077');
insert into transformer_info VALUES(1, 'localhost:9801');


insert into clusters values(2, 3, 'bob-cluster', 'Active', 0, 0);
insert into notebook_info VALUES(2, 'localhost:9998');
insert into spark_info VALUES(2, 'host.docker.internal:7077');
insert into transformer_info VALUES(2, 'localhost:9802');


insert into clusters values(3, 4, 'charlie-cluster', 'Active', 0, 0);
insert into notebook_info VALUES(3, 'localhost:9998');
insert into spark_info VALUES(3, 'host.docker.internal:7077');
insert into transformer_info VALUES(3, 'localhost:9803');