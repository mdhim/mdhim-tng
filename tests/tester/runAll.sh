#!/bin/sh

echo "type INT"

rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestBasic.txt -t1 -q -p/lustre/lscratch1/jhr/
rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestNext.txt -t1 -q -p/lustre/lscratch1/jhr/

echo "type LONG"

rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestBasic.txt -t2 -q -p/lustre/lscratch1/jhr/
rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestNext.txt -t2 -q -p/lustre/lscratch1/jhr/

echo "type FLOAT"

rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestBasic.txt -t3 -q -p/lustre/lscratch1/jhr/
rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestNext.txt -t3 -q -p/lustre/lscratch1/jhr/

echo "type DOUBLE"

rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestBasic.txt -t4 -q -p/lustre/lscratch1/jhr/
rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestNext.txt -t4 -q -p/lustre/lscratch1/jhr/

echo "type STRING"

rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestBasic.txt -t5 -q -p/lustre/lscratch1/jhr/
rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestNext.txt -t5 -q -p/lustre/lscratch1/jhr/
rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestLarge.txt -t5 -q -p/lustre/lscratch1/jhr/

echo "type BYTE"

rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestBasic.txt -t6 -q -p/lustre/lscratch1/jhr/
rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestNext.txt -t6 -q -p/lustre/lscratch1/jhr/
rm -rf /lustre/lscratch1/jhr/mdhimTst* ; aprun -n 2 ./mdhimtst -d2 -finTestLarge.txt -t6 -q -p/lustre/lscratch1/jhr/
