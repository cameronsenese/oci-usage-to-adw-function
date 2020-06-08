FROM oraclelinux:7-slim
RUN mkdir /function
WORKDIR /function
ADD requirements.txt /function/
RUN  yum -y install oracle-release-el7 && \
     yum -y install oracle-softwarecollection-release-el7 && \
     yum-config-manager --enable software_collections && \
     yum-config-manager --enable ol7_latest ol7_optional_latest ol7_addons && \
     yum-config-manager --disable ol7_ociyum_config && \
     yum -y install scl-utils && \
     yum -y install rh-python36 && \
     yum -y install gcc && \
     yum -y install oracle-instantclient19.3-basiclite && \
     export PATH=$PATH:/opt/rh/rh-python36/root/usr/bin && \
     rm -rf /var/cache/yum && \
     pip3 install --no-cache --no-cache-dir -r requirements.txt && rm -fr ~/.cache/pip /tmp* requirements.txt func.yaml Dockerfile .venv && \
     groupadd --gid 1000 fn && \
     adduser --uid 1000 --gid fn fn
ADD . /function/
ENTRYPOINT ["/opt/rh/rh-python36/root/usr/bin/fdk", "/function/func.py", "handler"]