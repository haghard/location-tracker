

i=5580
j=5680

while [ $i -ne $j ]
do
  i=$(($i+1))
  grpcurl -d '{"vehicleId":'${i}',"lon":1.1,"lat":1.1}' -plaintext 127.0.0.2:8080 com.rides.VehicleService/PostLocation;
done
