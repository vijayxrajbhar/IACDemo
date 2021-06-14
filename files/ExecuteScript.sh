#!/bin/bash
# Print Hello world message
echo "Hello World!"
directory_name="/etc/nginx/conf.d/web2py"

if [ -d $directory_name ]
then
    echo "Directory already exists"
else
    echo "Direactory Created"
    sudo mkdir $directory_name
fi
echo '
gzip_static on;
gzip_http_version   1.1;
gzip_proxied        expired no-cache no-store private auth;
gzip_disable        "MSIE [1-6]\.";
gzip_vary           on;
' > /etc/nginx/conf.d/web2py/gzip_static.conf

echo '
gzip on;
gzip_disable "msie6";
gzip_vary on;
gzip_proxied any;
gzip_comp_level 6;
gzip_buffers 16 8k;
gzip_http_version 1.1;
gzip_types text/plain text/css application/json application/x-javascript text/xml application/xml application/xml+rss text/javascript;
' > /etc/nginx/conf.d/web2py/gzip.conf

echo 'server {
        listen          80;
        server_name     $hostname;
        ###to enable correct use of response.static_version
        location ~* ^/(\w+)/static(?:/_[\d]+\.[\d]+\.[\d]+)?/(.*)$ {
            alias /home/www-data/web2py/applications/$1/static/$2;
            expires max;
            ### if you want to use pre-gzipped static files (recommended)
            ### check scripts/zip_static_files.py and remove the comments
            # include /etc/nginx/conf.d/web2py/gzip_static.conf;
        }
        ###
        ###if you use something like myapp = dict(languages=['en', 'it', 'jp'], default_language='en') in your routes.py
        #location ~* ^/(\w+)/(en|it|jp)/static/(.*)$ {
        #    alias /home/www-data/web2py/applications/$1/;
        #    try_files static/$2/$3 static/$3 =404;
        #}
        ###
        
        location / {
            #uwsgi_pass      127.0.0.1:9001;
            uwsgi_pass      unix:///tmp/uwsgitemp/web2py.socket;
            include         uwsgi_params;
            uwsgi_param     UWSGI_SCHEME $scheme;
            uwsgi_param     SERVER_SOFTWARE    nginx/$nginx_version;
            ###remove the comments to turn on if you want gzip compression of your pages
            # include /etc/nginx/conf.d/web2py/gzip.conf;
            ### end gzip section
            ### remove the comments if you use uploads (max 10 MB)
            #client_max_body_size 10m;
            ###
        }
}
server {
        listen 443 default_server ssl;
        server_name     $hostname;
        ssl_certificate         /etc/nginx/ssl/server.crt;
        ssl_certificate_key     /etc/nginx/ssl/server.key;
        ssl_prefer_server_ciphers on;
        ssl_session_cache shared:SSL:10m;
        ssl_session_timeout 10m;
        ssl_ciphers ECDHE-RSA-AES256-SHA:DHE-RSA-AES256-SHA:DHE-DSS-AES256-SHA:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA;
        ssl_protocols TLSv1 TLSv1.1 TLSv1.2;
        keepalive_timeout    70;
        location / {
            #uwsgi_pass      127.0.0.1:9001;
            uwsgi_pass      unix:///tmp/uwsgitemp/web2py.socket;
            include         uwsgi_params;
            uwsgi_param     UWSGI_SCHEME $scheme;
            uwsgi_param     SERVER_SOFTWARE    nginx/$nginx_version;
            ###remove the comments to turn on if you want gzip compression of your pages
            # include /etc/nginx/conf.d/web2py/gzip.conf;
            ### end gzip section
            ### remove the comments if you want to enable uploads (max 10 MB)
            #client_max_body_size 10m;
            ###
        }
        ###to enable correct use of response.static_version
        location ~* ^/(\w+)/static(?:/_[\d]+\.[\d]+\.[\d]+)?/(.*)$ {
            alias /home/www-data/web2py/applications/$1/static/$2;
            expires max;
            ### if you want to use pre-gzipped static files (recommended)
            ### check scripts/zip_static_files.py and remove the comments
            # include /etc/nginx/conf.d/web2py/gzip_static.conf;
        }
        ###
}' > /etc/nginx/sites-available/web2py


#uWSGI Emperor
echo '#uWSGI Emperor
[Unit]
Description = uWSGI Emperor
After = syslog.target
[Service]
ExecStart = /usr/local/bin/uwsgi --ini /etc/uwsgi/web2py.ini
RuntimeDirectory = uwsgi
Restart = always
KillSignal = SIGQUIT
Type = notify
StandardError = syslog
NotifyAccess = all
[Install]
WantedBy = multi-user.target
' > /etc/systemd/system/emperor.uwsgi.service

echo '<uwsgi>
    <socket>/tmp/uwsgitemp/web2py.socket</socket>
    <pythonpath>/home/www-data/web2py/</pythonpath>
    <mount>/=wsgihandler:application</mount>
    <plugins>python</plugins>
    <master/>
    <processes>4</processes>
    <harakiri>60</harakiri>
    <reload-mercy>8</reload-mercy>
    <cpu-affinity>1</cpu-affinity>
    <stats>/tmp/stats.socket</stats>
    <max-requests>2000</max-requests>
    <limit-as>512</limit-as>
    <reload-on-as>256</reload-on-as>
    <reload-on-rss>192</reload-on-rss>
    <uid>www-data</uid>
    <pidfile2>/tmp/webfile.id</pidfile2>
    <gid>www-data</gid>
    <no-orphans/>
</uwsgi>' >/etc/uwsgi/apps-available/web2py.xml

sudo ln -s /etc/nginx/sites-available/web2py /etc/nginx/sites-enabled/web2py
sudo ln -s /etc/uwsgi/apps-available/web2py.xml /etc/uwsgi/apps-enabled/web2py.xml
sudo rm /etc/nginx/sites-enabled/default

directory_ssl="/etc/nginx/ssl"

if [ -d $directory_ssl ]
then
    echo "Directory already exists"
else
    echo "Direactory Created"
    sudo mkdir $directory_ssl
fi


directory_uwsgitemp="/tmp/uwsgitemp"

if [ -d $directory_uwsgitemp ]
then
    echo "Directory already exists"
else
    echo "Direactory Created"
    sudo mkdir $directory_uwsgitemp
fi

sudo chown -R www-data:www-data /tmp/uwsgitemp

directory_www_data="/home/www-data"

if [ -d $directory_www_data ]
then
    echo "Directory already exists"
else
    echo "Direactory Created"
    sudo mkdir $directory_www_data
fi
sudo chown -R www-data:www-data /home/www-data
cd /home/www-data
sudo wget http://web2py.com/examples/static/web2py_src.zip
sudo unzip web2py_src.zip
sudo mv web2py/handlers/wsgihandler.py web2py/wsgihandler.py
sudo rm web2py_src.zip
sudo chown -R www-data:www-data web2py
cd /home/www-data/web2py
sudo -u www-data python -c "from gluon.main import save_password; save_password('web2py',443)"

#Run the following to restart Nginx:
sudo /etc/init.d/nginx stop
sudo /etc/init.d/nginx start
sudo /etc/init.d/uwsgi stop
sudo /etc/init.d/uwsgi start

