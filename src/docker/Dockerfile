# Automatically generated Dockerfile 
# - Build ${timestamp}
# - Lucene/Solr version ${currentDevelopmentVersion}
# - Mtas release ${currentDevelopmentRelease} 
#
# To run this image after installing Docker, use the following command:
# 

FROM ubuntu:16.04
MAINTAINER Matthijs Brouwer

LABEL mtas.timestamp="${timestamp}"
LABEL mtas.lucene="${currentDevelopmentVersion}"
LABEL mtas.release="${currentDevelopmentRelease}"

EXPOSE 8983 80

USER root 

WORKDIR "/root"

RUN mkdir lib && mkdir data && mkdir data/mtas

COPY solrconfig.xml /root/data/ 
COPY mtas.xml /root/data/
COPY mtas/demo_folia.xml /root/data/mtas/
COPY mtas/demo_tei.xml /root/data/mtas/
COPY schemaBasic.xml /root/data/
COPY schemaFull.xml /root/data/

ADD http://archive.apache.org/dist/lucene/solr/${currentDevelopmentVersion}/solr-${currentDevelopmentVersion}.tgz /root/
ADD http://apache.cs.uu.nl/commons/math/binaries/commons-math3-3.6.1-bin.tar.gz /root/lib/
ADD https://github.com/meertensinstituut/mtas/releases/download/${currentDevelopmentRelease}/mtas-${currentDevelopmentVersion}.jar /root/lib/
ADD https://code.jquery.com/jquery-3.1.1.min.js /root/lib/

RUN tar xzf lib/commons-math3-3.6.1-bin.tar.gz -C lib commons-math3-3.6.1/commons-math3-3.6.1.jar --strip-components=1 && rm lib/commons-math3-3.6.1-bin.tar.gz

RUN apt-get update && apt-get install -y \
lsof \
software-properties-common \
python-software-properties \
apache2

RUN add-apt-repository -y ppa:webupd8team/java \
&& apt-get update \
&& echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections \
&& apt-get install -y oracle-java8-installer

RUN rm -rf /var/lib/apt/lists/*

RUN tar xzf solr-${currentDevelopmentVersion}.tgz solr-${currentDevelopmentVersion}/bin/install_solr_service.sh --strip-components=2 \
&& bash ./install_solr_service.sh solr-${currentDevelopmentVersion}.tgz && rm install_solr_service.sh && rm -rf solr-${currentDevelopmentVersion}.tgz

RUN service apache2 stop \
&& a2enmod proxy \
&& a2enmod proxy_http \
&& a2enmod proxy_ajp \
&& a2enmod rewrite \
&& a2enmod deflate \
&& a2enmod headers \
&& a2enmod proxy_balancer \
&& a2enmod proxy_connect \
&& a2enmod proxy_html \
&& sed -i '/<\/VirtualHost>/ i ProxyPass /solr http://localhost:8983/solr\nProxyPassReverse /solr http://localhost:8983/solr' /etc/apache2/sites-enabled/000-default.conf

RUN printf "service solr start\nservice apache2 start\n" > /start.sh && chmod 755 /start.sh

RUN mkdir demo1 && mkdir demo1/lib && mkdir demo1/conf && echo "name=demo1" > demo1/core.properties \
&& cp lib/commons-math3-3.6.1.jar demo1/lib/ && cp lib/mtas-${currentDevelopmentVersion}.jar demo1/lib/ \
&& cp data/solrconfig.xml demo1/conf/ && cp data/schemaBasic.xml demo1/conf/schema.xml \
&& cp -r data/mtas demo1/conf/ && cp data/mtas.xml demo1/conf/ \  
&& chmod -R 777 demo1 && cp -rp demo1 demo2 \
&& cp data/schemaFull.xml demo2/conf/schema.xml && echo "name=demo2" > demo2/core.properties\
&& mv demo1 /var/solr/data/ && mv demo2 /var/solr/data/

CMD bash -C '/start.sh'; 'bash'







