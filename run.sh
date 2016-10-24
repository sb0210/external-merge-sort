cd amlayer
make clean
make

cd ../pflayer;
make clean cleaninp
make inp mytestpf
echo -n "Merge sort bplus" > ../time.txt
for i in `seq 1 10`
do 
./generate_input inp$i >input$i.txt
./mytestpf inp$i >> ../time.txt
mv 0.* output$i.txt
../amlayer/a.out input1.txt >> ../time.txt

done
