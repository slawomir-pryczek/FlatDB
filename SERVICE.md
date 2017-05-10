#FlatDB Service
Setting up as a service is much more convinient than using screen and auto-restart by loop. FlatDB is using auto-restart and will put logs outside
of syslog, for convinience.

First you need to create a new file called *flatdb.service* in /etc/systemd/system
```ini
#systemctl daemon-reload -> After changing service config
#systemctl enable flatdb -> After you create file remember to issue this 

[Unit]
Description=FlatDB K/V datastore server

[Service]
LimitNOFILE=524288
LimitMEMLOCK=1073741824
ExecStart=/bin/sh -c 'cd /home/flatdb/; export GODEBUG=gctrace=1; started=`date --rfc-3339=seconds`; echo Starting Flatdb $started; ./flatdb 1>"log-$started.txt" 2>"error-$started.log.txt";'
Type=simple
PrivateNetwork=false
PrivateTmp=false
ProtectSystem=false
ProtectHome=false
KillMode=control-group
Restart=always
DefaultTasksMax=65536
TasksMax=65536

[Install]
WantedBy=multi-user.target
```

Then, create new directory /home/flatdb/
```sh
mkdir /home/flatdb/
```

Copy flatdb server executable, conf.json and server-status.html here, then
```sh
cd /home/flatdb/
chmod 0777 ../flatdb
chmod 0777 flatdb
```

Last step is to reload the systemd config, enable service to be started automatically and start it
```sh
systemctl daemon-reload
systemctl enable flatdb
service flatdb start
```

After this, the service will be started on server boot, and will also automatically re-start in case of failure.
