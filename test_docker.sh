wget https://repo.mysql.com//mysql80-community-release-el8-3.noarch.rpm


docker run -ti --rm -v /home/ec2-user/workspace/data-extractor/odbc-source/target/wheels/:/opt/lib registry.access.redhat.com/ubi8/ubi-minimal:8.7-1085