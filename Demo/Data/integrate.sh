NUM=$1;
FNAME=WifiStatusLarge_$NUM.csv;

rm $FNAME
for i in $(seq 1 $NUM)
do
    cat WifiStatusTotal.csv >> $FNAME;
done
