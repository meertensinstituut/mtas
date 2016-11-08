# Automatically generated Dockerfile 
# - Build 2016-11-07 18:42
# - Lucene/Solr version 6.2.0
# - Mtas release 20161027 
#
# To run this image after installing Docker, use the following command:
# 

FROM ubuntu:16.04
MAINTAINER Matthijs Brouwer

LABEL mtas.timestamp="2016-11-07 18:42"
LABEL mtas.lucene="6.2.0"
LABEL mtas.release="20161027"

EXPOSE 8983 80
  
USER root 

WORKDIR "/root" 

RUN mkdir lib && mkdir data && mkdir data/mtas && mkdir folia-samples && mkdir site && mkdir site/js && mkdir site/css

ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/solrconfig.xml /root/data/ 
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/mtas.xml /root/data/
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/mtas/demo_folia.xml /root/data/mtas/
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/mtas/demo_tei.xml /root/data/mtas/
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/schemaBasic.xml /root/data/
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/schemaFull.xml /root/data/
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/folia-samples/beets1.xml.gz /root/folia-samples/
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/folia-samples/beets2.xml.gz /root/folia-samples/
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/folia-samples/beets3.xml.gz /root/folia-samples/
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/site/index.html /root/site/ 
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/site/example_demo1.html /root/site/ 
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/site/example_demo2.html /root/site/ 
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/site/css/style.css /root/site/css/
ADD https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/site/js/solr.js /root/site/js/

ADD http://apache.cs.uu.nl/commons/math/binaries/commons-math3-3.6.1-bin.tar.gz /root/lib/
ADD https://github.com/meertensinstituut/mtas/releases/download/20161027/mtas-6.2.0.jar /root/lib/
ADD http://code.jquery.com/jquery-3.1.1.min.js /root/site/js/

RUN tar xzf lib/commons-math3-3.6.1-bin.tar.gz -C lib commons-math3-3.6.1/commons-math3-3.6.1.jar --strip-components=1 && rm lib/commons-math3-3.6.1-bin.tar.gz

RUN apt-get update && apt-get install -y \
lsof \
software-properties-common \
python-software-properties \
apache2 \
curl 

RUN solrurl=$(curl -s 'http://www.apache.org/dyn/closer.lua/lucene/solr/6.2.0/solr-6.2.0.tgz' |   grep -o '<strong>[^<]*</strong>' |   sed 's/<[^>]*>//g' |   head -1) \
&& curl -o /root/solr-6.2.0.tgz -O $solrurl

RUN mathurl=$(curl -s 'http://www.apache.org/dyn/closer.lua/commons/math/binaries/commons-math3-3.6.1-bin.tar.gz' |   grep -o '<strong>[^<]*</strong>' |   sed 's/<[^>]*>//g' |   head -1) \
&& curl -o /root/lib/commons-math3-3.6.1-bin.tar.gz -O $mathurl

RUN add-apt-repository -y ppa:webupd8team/java \
&& apt-get update \
&& echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections \
&& apt-get install -y oracle-java8-installer

RUN rm -rf /var/lib/apt/lists/*

RUN tar xzf solr-6.2.0.tgz solr-6.2.0/bin/install_solr_service.sh --strip-components=2 \
&& bash ./install_solr_service.sh solr-6.2.0.tgz && rm install_solr_service.sh && rm -rf solr-6.2.0.tgz

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

RUN rm /var/www/html/index.html \
&& mkdir /var/www/html/demo \ 
&& cp -rp folia-samples /var/www/html/demo/ \
&& gunzip -r /var/www/html/demo \
&& cp -rp site/* /var/www/html/ \
&& chmod -R 755 /var/www/html 

RUN printf "service solr start\nservice apache2 start\n" > /start.sh && chmod 755 /start.sh

RUN mkdir demo1 && mkdir demo1/lib && mkdir demo1/conf && echo "name=demo1" > demo1/core.properties \
&& cp lib/commons-math3-3.6.1.jar demo1/lib/ && cp lib/mtas-6.2.0.jar demo1/lib/ \
&& cp data/solrconfig.xml demo1/conf/ && cp data/schemaBasic.xml demo1/conf/schema.xml \
&& cp -r data/mtas demo1/conf/ && cp data/mtas.xml demo1/conf/ \  
&& chmod -R 777 demo1 && cp -rp demo1 demo2 \
&& cp data/schemaFull.xml demo2/conf/schema.xml && echo "name=demo2" > demo2/core.properties\
&& mv demo1 /var/solr/data/ && mv demo2 /var/solr/data/

CMD bash -C '/start.sh'; 'bash'





