# LIB_VER=0.3.1

# build-docker-image:
# 	docker build -t maturin-odbc:latest .

# build-package:
# 	docker run --rm -v $(shell pwd):/io -v $(shell pwd)/oracle_rs:/oracle_rs maturin-odbc build --release

# install-package: build-package
# 	pip3 install --force-reinstall target/wheels/odbc_source-${LIB_VER}-cp37-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl

# publish-package: build-package
# 	aws s3 cp target/wheels/odbc_source-${LIB_VER}-cp37-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl s3://techx-deployment-artifact/extraction/

# download_oracle:
# 	rm -rf /tmp/instantclient_21_12 &&\
# 	rm -rf /tmp/oracle.zip &&\
# 	wget https://download.oracle.com/otn_software/linux/instantclient/2112000/instantclient-basiclite-linux.x64-21.12.0.0.0dbru.zip -O /tmp/oracle.zip &&\
# 	unzip /tmp/oracle.zip -d /tmp &&\
# 	ls -l /tmp/instantclient_21_12 &&\
# 	mkdir -p /tmp/instantclient_21_12/lib &&\
# 	cp /tmp/instantclient_21_12/*.so* /tmp/instantclient_21_12/lib

build-package:
	docker build -t extraction-libs:latest . \
	--network host \
		--build-arg HTTP_PROXY=http://localhost:3128 \
		--build-arg HTTPS_PROXY=http://localhost:3128

get-libs:
	mkdir -p ./libs
	docker create --name temp-container extraction-libs:latest
	docker cp temp-container:/libs/ ./
	docker rm temp-container