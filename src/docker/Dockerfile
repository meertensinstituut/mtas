# Automatically generated Dockerfile 
# - Build ${timestamp}
# - Lucene/Solr version ${currentDevelopmentVersion}
# - Mtas release ${currentDevelopmentRelease} 
#

FROM ubuntu:16.04
MAINTAINER Matthijs Brouwer, Meertens Institute

EXPOSE 8983 80
  
USER root 

WORKDIR "/root" 

RUN mkdir lib 

ADD https://github.com/meertensinstituut/mtas/releases/download/${currentDevelopmentRelease}/mtas-${currentDevelopmentVersion}.jar /root/lib/

RUN apt-get update && apt-get install -y lsof software-properties-common python-software-properties apache2 curl subversion \
&& solrurl=$(curl -s 'http://www.apache.org/dyn/closer.lua/lucene/solr/${currentDevelopmentVersion}/solr-${currentDevelopmentVersion}.tgz' |   grep -o '<strong>[^<]*</strong>' |   sed 's/<[^>]*>//g' |   head -1) \
&& curl -o /root/solr-${currentDevelopmentVersion}.tgz -O $solrurl \
&& mathurl=$(curl -s 'http://www.apache.org/dyn/closer.lua/commons/math/binaries/commons-math3-3.6.1-bin.tar.gz' |   grep -o '<strong>[^<]*</strong>' |   sed 's/<[^>]*>//g' |   head -1) \
&& curl -o /root/lib/commons-math3-3.6.1-bin.tar.gz -O $mathurl \
&& tar xzf lib/commons-math3-3.6.1-bin.tar.gz -C lib commons-math3-3.6.1/commons-math3-3.6.1.jar --strip-components=1 \
&& rm lib/commons-math3-3.6.1-bin.tar.gz \
&& svn export https://github.com/meertensinstituut/mtas/trunk/docker/ data \
&& add-apt-repository -y ppa:webupd8team/java \
&& apt-get update \
&& echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections \
&& apt-get install -y oracle-java8-installer \
&& rm -rf /var/lib/apt/lists/* \
&& tar xzf solr-${currentDevelopmentVersion}.tgz solr-${currentDevelopmentVersion}/bin/install_solr_service.sh --strip-components=2 \
&& bash ./install_solr_service.sh solr-${currentDevelopmentVersion}.tgz && rm install_solr_service.sh && rm -rf solr-${currentDevelopmentVersion}.tgz \
&& service apache2 stop \
&& a2enmod proxy \
&& a2enmod proxy_http \
&& a2enmod proxy_ajp \
&& a2enmod rewrite \
&& a2enmod deflate \
&& a2enmod headers \
&& a2enmod proxy_balancer \
&& a2enmod proxy_connect \
&& a2enmod proxy_html \
&& a2enmod xml2enc \
&& sed -i '/<\/VirtualHost>/ i ProxyPass /solr http://localhost:8983/solr\nProxyPassReverse /solr http://localhost:8983/solr' /etc/apache2/sites-enabled/000-default.conf \
&& rm -rf /var/www/html/* \
&& mkdir /var/www/html/demo \ 
&& cp -rp data/folia-samples /var/www/html/demo/ \
&& gunzip -r /var/www/html/demo \
&& cp -rp data/site/* /var/www/html/ \
&& chmod -R 755 /var/www/html \
&& printf "echo\n" >> /start.sh \
&& printf "echo \"================ Mtas -- Multi Tier Annotation Search =================\"\n" >> /start.sh \
&& printf "echo \"  Timestamp ${timestamp}\"\n" >> /start.sh \
&& printf "echo \"  Lucene/Solr version ${currentDevelopmentVersion}\"\n" >> /start.sh \
&& printf "echo \"  Mtas release ${currentDevelopmentRelease}\"\n" >> /start.sh \
&& printf "echo \"  See https://meertensinstituut.github.io/mtas/ for more information\"\n" >> /start.sh \
&& printf "echo \"=======================================================================\"\n" >> /start.sh \
&& printf "echo\n" >> /start.sh \
&& printf "service solr start\nservice apache2 start\n" >> /start.sh \
&& chmod 755 /start.sh \
&& mkdir demo1 && mkdir demo1/lib && mkdir demo1/conf \
&& echo "name=demo1" > demo1/core.properties \
&& cp lib/commons-math3-3.6.1.jar demo1/lib/ \
&& cp lib/mtas-${currentDevelopmentVersion}.jar demo1/lib/ \
&& cp data/solrconfig.xml demo1/conf/ \
&& cp data/schemaBasic.xml demo1/conf/schema.xml \
&& cp -r data/mtas demo1/conf/ && cp data/mtas.xml demo1/conf/ \  
&& chmod -R 777 demo1 \
&& cp -rp demo1 demo2 \
&& cp data/schemaFull.xml demo2/conf/schema.xml \
&& echo "name=demo2" > demo2/core.properties\
&& mv demo1 /var/solr/data/ \
&& mv demo2 /var/solr/data/

CMD bash -C '/start.sh'; 'bash'

