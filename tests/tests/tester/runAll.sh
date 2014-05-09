#!/bin/sh

echo "type INT"

rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestBasic.txt -t1 -q -p./
rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestNext.txt -t1 -q -p./

echo "type LONG"

rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestBasic.txt -t2 -q -p./
rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestNext.txt -t2 -q -p./

echo "type FLOAT"

rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestBasic.txt -t3 -q -p./
rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestNext.txt -t3 -q -p./

echo "type DOUBLE"

rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestBasic.txt -t4 -q -p./
rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestNext.txt -t4 -q -p./

echo "type STRING"

rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestBasic.txt -t5 -q -p./
rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestNext.txt -t5 -q -p./
rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestLarge.txt -t5 -q -p./

echo "type BYTE"

rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestBasic.txt -t6 -q -p./
rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestNext.txt -t6 -q -p./
rm -rf ./mdhimTst* ; srun -N 2 ./mdhimtst -finTestLarge.txt -t6 -q -p./
