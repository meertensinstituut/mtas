# Automatically generated Dockerfile 
# - Build 2018-07-08 09:42
# - Lucene/Solr version 7.4.0
# - Mtas release 7.4.0.0 
#

FROM ubuntu:16.04
MAINTAINER Matthijs Brouwer, Textexploration.org

EXPOSE 8983 80
  
USER root

WORKDIR "/root" 

RUN mkdir lib 

ADD https://github.com/textexploration/mtas/releases/download/v7.4.0.0/mtas-7.4.0.0.jar /root/lib/

RUN apt-get update && apt-get install -y locales lsof software-properties-common python-software-properties apache2 curl subversion && \
    locale-gen en_US.UTF-8 en_US && update-locale LANG=en_US.UTF-8 LANGUAGE=en_US:en
            
RUN mathurl=$(curl -s 'http://www.apache.org/dyn/closer.lua/commons/math/binaries/commons-math3-3.6.1-bin.tar.gz' |   grep -o '<strong>[^<]*</strong>' |   sed 's/<[^>]*>//g' |   head -1) && \
    if echo "$mathurl" | grep -q '^.*[^ ].*$'; then \
      curl -f -o /root/lib/commons-math3-3.6.1-bin.tar.gz -O $mathurl || true; \
    fi && \
    if [ ! -f /root/lib/commons-math3-3.6.1-bin.tar.gz ]; then \
      echo "Commons-math3 not found in mirror, falling back to apache archive"; \ 
      mathurl="http://archive.apache.org/dist/commons/math/binaries/commons-math3-3.6.1-bin.tar.gz"; \
      curl -f -o /root/lib/commons-math3-3.6.1-bin.tar.gz -O $mathurl; \
    fi && \ 
    tar xzf lib/commons-math3-3.6.1-bin.tar.gz -C lib commons-math3-3.6.1/commons-math3-3.6.1.jar --strip-components=1 && \
    rm lib/commons-math3-3.6.1-bin.tar.gz
    
RUN svn export https://github.com/textexploration/mtas/trunk/docker/ data 

RUN add-apt-repository -y ppa:webupd8team/java && \
    apt-get update && \
    echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer && \
    rm -rf /var/lib/apt/lists/*

RUN solrurl=$(curl -s 'http://www.apache.org/dyn/closer.lua/lucene/solr/7.4.0/solr-7.4.0.tgz' |   grep -o '<strong>[^<]*</strong>' |   sed 's/<[^>]*>//g' |   head -1) && \
    if echo "$solrurl" | grep -q '^.*[^ ].*$'; then \
      curl -f -o /root/solr-7.4.0.tgz -O $solrurl || true; \
    fi && \
    if [ ! -f /root/solr-7.4.0.tgz ]; then \
      echo "Solr 7.4.0 not found in mirror, falling back to apache archive"; \ 
      solrurl="http://archive.apache.org/dist/lucene/solr/7.4.0/solr-7.4.0.tgz"; \
      curl -f -o /root/solr-7.4.0.tgz -O $solrurl; \ 
    fi && \    
    tar xzf solr-7.4.0.tgz solr-7.4.0/bin/install_solr_service.sh --strip-components=2 && \
    bash ./install_solr_service.sh solr-7.4.0.tgz && rm install_solr_service.sh && rm -rf solr-7.4.0.tgz
    
RUN service apache2 stop && \
    echo "ServerName localhost" | tee /etc/apache2/conf-available/fqdn.conf && \
    a2enmod proxy && \
    a2enmod proxy_http && \
    a2enmod proxy_ajp && \
    a2enmod rewrite && \
    a2enmod deflate && \
    a2enmod headers && \
    a2enmod proxy_balancer && \
    a2enmod proxy_connect && \
    a2enmod proxy_html && \
    a2enmod xml2enc && \
    a2enconf fqdn && \
    sed -i '/<\/VirtualHost>/ i ProxyPass /solr http://localhost:8983/solr\nProxyPassReverse /solr http://localhost:8983/solr' /etc/apache2/sites-enabled/000-default.conf && \
    rm -rf /var/www/html/* && \
    mkdir /var/www/html/demo && \
    cp -rp data/*-samples /var/www/html/demo/ && \
    gunzip -r /var/www/html/demo && \
    cp -rp data/site/* /var/www/html && \
    chmod -R 755 /var/www/html && \
    printf "echo\n" >> /start.sh && \
    printf "echo \"================ Mtas -- Multi Tier Annotation Search =================\"\n" >> /start.sh && \
    printf "echo \"  Timestamp 2018-07-08 09:42\"\n" >> /start.sh && \
    printf "echo \"  Lucene/Solr version 7.4.0\"\n" >> /start.sh && \
    printf "echo \"  Mtas release 7.4.0.0\"\n" >> /start.sh && \
    printf "echo \"  See https://textexploration.github.io/mtas/ for more information\"\n" >> /start.sh && \
    printf "echo \"=======================================================================\"\n" >> /start.sh && \
    printf "echo\n" >> /start.sh && \
    printf "service solr start\nservice apache2 start\n" >> /start.sh && \
    chmod 755 /start.sh && \
    mkdir demo1 && mkdir demo1/lib && mkdir demo1/conf && \
    echo "name=demo1" > demo1/core.properties && \
    cp lib/commons-math3-3.6.1.jar demo1/lib/ && \
    cp lib/mtas-7.4.0.0.jar demo1/lib/ && \
    cp data/solrconfig.xml demo1/conf/ && \
    cp data/schemaBasic.xml demo1/conf/schema.xml && \
    cp -r data/mtas demo1/conf/ && cp data/mtas.xml demo1/conf/ && \
    chmod -R 777 demo1 && \
    cp -rp demo1 demo2 && \
    cp data/schemaFull.xml demo2/conf/schema.xml && \
    echo "name=demo2" > demo2/core.properties && \
    cp -rp demo1 demo3 && \
    cp data/schemaFull.xml demo3/conf/schema.xml && \
    echo "name=demo3" > demo3/core.properties && \
    cp -rp demo1 demo4 && \
    cp data/schemaFull.xml demo4/conf/schema.xml && \
    echo "name=demo4" > demo4/core.properties && \
    mv demo1 /var/solr/data/ && \
    mv demo2 /var/solr/data/ && \
    mv demo3 /var/solr/data/ && \
    mv demo4 /var/solr/data/

CMD bash -C '/start.sh'; 'bash'
