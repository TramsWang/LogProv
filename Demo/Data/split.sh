declare -i n=1;
NUM=10;
for i in $(seq 1 ${NUM})
do
    cat WifiStatusTotal.csv | head --lines=${i}0K > WifiStatus_${i}0K.csv
    echo ${i}0K Done.
done
