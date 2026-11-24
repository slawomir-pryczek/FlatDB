echo \# This file will build golang server and move configuration files
echo \# to bin directory, under Linux.
echo \#
echo \#

SCRIPT=`realpath $0`
SCRIPTPATH=`dirname $SCRIPT`

cd ../go/flatdb
unlink flatdb 2>/dev/null
unlink ../../bin/flatdb  2>/dev/null
unlink ../../bin/flatdb-linux  2>/dev/null
unlink ../../bin/server-status.html 2>/dev/null
unlink ../../bin/conf.json 2>/dev/null

#go build -a -v flatdb.go handler_storage.go
GOAMD64=v3 CGO_ENABLED=0 go build -ldflags="-s -w" -o flatdb .
mv flatdb ../../bin/flatdb


cp -f ./server-status.html ../../bin/server-status.html
cp -f ./conf.json ../../bin/conf.json

echo "Finished!"