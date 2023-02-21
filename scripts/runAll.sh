
#https://gist.github.com/amencke/95e01db205ebcbb07ce014c2439df538#file-gistfile1-sh
#https://linuxize.com/post/bash-if-else-statement/

i=6480
j=6780

while [ $i -ne $j ]
do
  i=$(($i+1))
  grpcurl -d '{"vehicleId":'${i}',"lon":1.1,"lat":1.1}' -plaintext 127.0.0.1:8080 com.rides.VehicleService/PostLocation;
done
